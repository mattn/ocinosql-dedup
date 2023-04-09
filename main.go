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

func run() int {
	var compartmentId, tableName string
	var createTable bool
	var hashkey bool
	var k string

	flag.StringVar(&compartmentId, "compartment-id", os.Getenv("OCI_NOSQL_COMPARTMENT_ID"), "compartment id")
	flag.StringVar(&tableName, "table-name", os.Getenv("OCI_NOSQL_TABLE_NAME"), "table name")
	flag.BoolVar(&createTable, "create-table", false, "create table")
	flag.BoolVar(&hashkey, "hashkey", false, "hash key")
	flag.StringVar(&k, "k", "id", "identify for the key")
	flag.Parse()

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

	buf := bufio.NewWriter(os.Stdout)
	dec := json.NewDecoder(os.Stdin)
	enc := json.NewEncoder(buf)
	for {
		var v map[string]interface{}
		err = dec.Decode(&v)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Fprintf(os.Stderr, "%v: %v\n", os.Args[0], err)
			return 1
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
		if err != nil || len(respGetRow.Value) == 0 {
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
			buf.Flush()
		}
	}
	return 0
}

func main() {
	os.Exit(run())
}
