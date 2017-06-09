package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	fileName := "/root/sys/vpn/var/log/asterisk/cdr-csv/Master.csv"
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	//
	reader := csv.NewReader(file)

	reader.Comma = ','
	record, err := reader.ReadAll()
	file.Close()
	errTruncate := os.Truncate(fileName, 0)
	if errTruncate != nil {
		panic(errTruncate.Error())
	}

	DBCon, err := sql.Open("mysql", "root:tut3-whoop@/asteriskcdrdb")
	if err != nil {
		panic(err.Error())
	}

	//fmt.Println("Record is", record[0], "and has", len(record[0]), "fields")

	for _, element := range record {
		if len(element) < 18 {
			fmt.Println("Empty Record")
			continue
		}
		var query string = "INSERT INTO cdr(calldate,clid,src,dst,dcontext,channel,dstchannel,lastapp,lastdata,duration,billsec,disposition,accountcode,uniqueid,userfield) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

		_, err2 := DBCon.Exec(query, element[9], element[4], element[1], element[2], element[3], element[5], element[6], element[7], element[8], element[12], element[13], element[14], element[0], element[16], element[17])
		if err2 != nil {

		}
		if err != nil {
			fmt.Println("Failed to Save: ", element[16])
		} else {
			fmt.Println("Saved : ", element[16])
		}

	}
	//Close database connection
	DBCon.Close()
}
