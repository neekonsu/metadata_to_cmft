package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
)

func readUrls(path string) []string {
	csvfile, _ := os.Open(path)
	r := csv.NewReader(csvfile)
	var outTable []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		outTable = append(outTable, record[2])
	}
	return outTable
}

func extractPath(url string) string {
	// example url: ftp://ftp.ncbi.nlm.nih.gov/geo/samples/GSM537nnn/GSM537697/suppl
	slashIndexAfterURL := 7 + strings.Index(url[7:], "/")
	return url[slashIndexAfterURL:]
}

func listBedFiles(serverConn *ftp.ServerConn, path string) []string {
	entries, err := serverConn.List(path)
	if err != nil {
		log.Fatal(err)
	}
	var filenames []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name, ".bed.gz") {
			filenames = append(filenames, entry.Name)
		}
	}
	return filenames
}

func main() {
	var csvPath string = os.Args[1]

	serverConn, err := ftp.Dial("ftp.ncbi.nlm.nih.gov:21", ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}

	err = serverConn.Login("anonymous", "anonymous")
	if err != nil {
		log.Fatal(err)
	}
	// export full links
	for _, url := range readUrls(csvPath) {
		path := extractPath(url)
		for _, bedFilename := range listBedFiles(serverConn, path) {
			fmt.Printf("ftp://ftp.ncbi.nlm.nih.gov/%s/%s\n", path[1:len(path)-1], bedFilename)
		}
	}
	// export plain BED files
	for _, url := range readUrls(csvPath) {
		path := extractPath(url)
		for _, bedFilename := range listBedFiles(serverConn, path) {
			fmt.Println(bedFilename[:len(bedFilename)-3])
		}
	}

	if err := serverConn.Quit(); err != nil {
		log.Fatal(err)
	}
}
