package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/oracle/oci-go-sdk/common"
	"github.com/oracle/oci-go-sdk/nosql"
)

const name = "ocinosql-dedup"

const version = "0.0.9"

var revision = "HEAD"

func run() int {
	var compartmentId, tableName string
	var createTable bool
	var hashkey bool
	var k string
	var verbose bool
	var showVersion bool

	flag.StringVar(&compartmentId, "compartment-id", os.Getenv("OCI_NOSQL_COMPARTMENT_ID"), "compartment id")
	flag.StringVar(&tableName, "table-name", os.Getenv("OCI_NOSQL_TABLE_NAME"), "table name")
	flag.BoolVar(&createTable, "create-table", false, "create table")
	flag.BoolVar(&hashkey, "hashkey", false, "hash key")
	flag.StringVar(&k, "k", "id", "identify for the key")
	flag.BoolVar(&verbose, "V", false, "verbose")
	flag.BoolVar(&showVersion, "v", false, "show version")
	flag.Parse()

	if showVersion {
		fmt.Println(version)
		os.Exit(0)
	}

	client, err := nosql.NewNosqlClientWithConfigurationProvider(common.DefaultConfigProvider())
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v: %v\n", os.Args[0], err)
		return 1
	}

	if createTable {
		_, err = client.CreateTable(context.Background(), nosql.CreateTableRequest{
			CreateTableDetails: nosql.CreateTableDetails{
				Name:          common.String(tableName),
				CompartmentId: common.String(compartmentId),
				DdlStatement:  common.String(fmt.Sprintf(`CREATE TABLE %s(id STRING, created_at TIMESTAMP(0), PRIMARY KEY(SHARD(id)))`, tableName)),
				TableLimits: &nosql.TableLimits{
					MaxReadUnits:    common.Int(1),
					MaxWriteUnits:   common.Int(1),
					MaxStorageInGBs: common.Int(1),
				},
			},
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v: %v\n", os.Args[0], err)
			return 1
		}
		return 0
	}

	var out io.Writer
	if verbose {
		out = io.MultiWriter(os.Stdout, os.Stderr)
	} else {
		out = os.Stdout
	}
	scanner := bufio.NewScanner(os.Stdin)
	enc := json.NewEncoder(out)
	for scanner.Scan() {
		var v map[string]interface{}
		text := scanner.Text()
		err = json.Unmarshal([]byte(text), &v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v: %v\n", os.Args[0], err)
			continue
		}
		vk, ok := v[k]
		if !ok {
			fmt.Fprintf(os.Stderr, "%v: %q not found\n", os.Args[0], k)
			continue
		}
		vks := fmt.Sprint(vk)
		if hashkey {
			vks = fmt.Sprintf("%x", sha256.Sum256([]byte(vks)))
		}

		respGetRow, err := client.GetRow(context.Background(), nosql.GetRowRequest{
			CompartmentId: common.String(compartmentId),
			TableNameOrId: common.String(tableName),
			Key:           []string{"id:" + vks},
		})
		if err != nil {
			continue
		}
		if len(respGetRow.Value) > 0 {
			continue
		}
		_, err = client.UpdateRow(context.Background(), nosql.UpdateRowRequest{
			TableNameOrId: common.String(tableName),
			UpdateRowDetails: nosql.UpdateRowDetails{
				CompartmentId: common.String(compartmentId),
				Value: map[string]interface{}{
					"id":         vks,
					"created_at": time.Now().Format(time.RFC3339),
				},
			},
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v: %v\n", os.Args[0], err)
			continue
		}
		enc.Encode(v)
	}
	return 0
}

func main() {
	os.Exit(run())
}
