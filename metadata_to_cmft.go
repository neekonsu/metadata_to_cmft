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
	r.Read()
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

func readMarks(path string) []string {
	csvfile, _ := os.Open(path)
	r := csv.NewReader(csvfile)
	r.Read()
	var outTable []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		outTable = append(outTable, record[3])
	}
	return outTable
}

func readSampleNames(path string) []string {
	csvfile, _ := os.Open(path)
	r := csv.NewReader(csvfile)
	r.Read()
	var outTable []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		outTable = append(outTable, record[1])
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

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}

func transpose(slice [][]string) [][]string {
	xl := len(slice[0])
	yl := len(slice)
	result := make([][]string, xl)
	for i := range result {
		result[i] = make([]string, yl)
	}
	for i := 0; i < xl; i++ {
		for j := 0; j < yl; j++ {
			result[i][j] = slice[j][i]
		}
	}
	return result
}

func main() {
	var csvPath string = os.Args[1]
	var tmpString string
	var fullLinks []string
	var bedNames []string
	var tmpCMFT [][]string
	var tmpWgetConf [][]string
	sampleNames := readSampleNames(csvPath)
	marks := readMarks(csvPath)
	tmpCMFT = append(tmpCMFT, sampleNames)
	tmpCMFT = append(tmpCMFT, marks)

	serverConn, err := ftp.Dial("ftp.ncbi.nlm.nih.gov:21", ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}

	err = serverConn.Login("anonymous", "anonymous")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("ftp server connected!")
	fmt.Println("extracting BED filenames and URLs")

	// TODO: make link extraction into goroutine to speed up process
	// export full links
	for _, url := range readUrls(csvPath) {
		path := extractPath(url)
		for _, bedFilename := range listBedFiles(serverConn, path) {
			tmpString = "ftp://ftp.ncbi.nlm.nih.gov/" + path[1:len(path)-1] + "/" + bedFilename
			fmt.Printf("extracted full link >>> %s\n", tmpString)
			fullLinks = append(fullLinks, tmpString)
		}
	}
	tmpWgetConf = append(tmpWgetConf, fullLinks)

	// export plain BED files
	for _, url := range readUrls(csvPath) {
		path := extractPath(url)
		for _, bedFilename := range listBedFiles(serverConn, path) {
			tmpString = bedFilename[:len(bedFilename)-3]
			fmt.Printf("extracted file name >>> %s\n", tmpString)
			bedNames = append(bedNames, tmpString)
		}
	}
	tmpCMFT = append(tmpCMFT, bedNames)

	fmt.Println("Done extracting BED filenames and URLs, disconnecting from ftp server")

	if err := serverConn.Quit(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Disconnected from server, writing files to current directory")

	file1, err := os.Create("cmft.tsv")
	checkError("error while exporting new cmft.csv:", err)
	defer file1.Close()

	writer1 := csv.NewWriter(file1)
	writer1.Comma = '\t'
	defer writer1.Flush()

	file2, err := os.Create("wget.conf")
	checkError("error while exporting new cmft.csv:", err)
	defer file2.Close()

	writer2 := csv.NewWriter(file2)
	writer2.Comma = '\t'
	defer writer2.Flush()

	tmpCMFT = transpose(tmpCMFT)
	tmpWgetConf = transpose(tmpWgetConf)

	for _, value := range tmpCMFT {
		err := writer1.Write(value)
		checkError("Cannot write to file", err)
	}

	for _, value := range tmpWgetConf {
		err := writer2.Write(value)
		checkError("Cannot write to file", err)
	}

	fmt.Println("All Done!")
}
