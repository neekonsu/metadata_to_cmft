package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jlaffaye/ftp"
)

// Read entries from one column of a csv to []string
func read(path string, index int8) []string {
	csvfile, _ := os.Open(path)
	r := csv.NewReader(csvfile)
	r.Read()
	var outTable []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		checkError("Unable to read line from csv: ", err)
		outTable = append(outTable, record[2])
	}
	return outTable
}

// Isolate filepath from full GEO url
func extractPath(url string) string {
	// example url: ftp://ftp.ncbi.nlm.nih.gov/geo/samples/GSM537nnn/GSM537697/suppl
	slashIndexAfterURL := 7 + strings.Index(url[7:], "/")
	return url[slashIndexAfterURL:]
}

// With existing ftp.serverConn, list BED files in ftp path as []string
func listBedFiles(serverConn *ftp.ServerConn, path string) []string {
	entries, err := serverConn.List(path)
	checkError("Failed to list path: "+path, err)
	var filenames []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name, ".bed.gz") {
			filenames = append(filenames, entry.Name)
		}
	}
	return filenames
}

// Boilerplate error code to avoid repetition.
func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}

// Copied code from stackoverflow: transpose 2d matrix
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
	var wg sync.WaitGroup
	sampleNames := read(csvPath, 1)
	marks := read(csvPath, 3)
	tmpCMFT = append(tmpCMFT, sampleNames)
	tmpCMFT = append(tmpCMFT, marks)

	serverConn, err := ftp.Dial("ftp.ncbi.nlm.nih.gov:21", ftp.DialWithTimeout(5*time.Second))
	checkError("Unable to dial ftp server: ", err)

	err = serverConn.Login("anonymous", "anonymous")
	checkError("Unable to authenticate ftp server: ", err)

	fmt.Println("ftp server connected!")
	fmt.Println("extracting BED filenames and URLs")

	// export full links
	wg.Add(1)
	go func() {
		for _, url := range read(csvPath, 2) {
			path := extractPath(url)
			for _, bedFilename := range listBedFiles(serverConn, path) {
				tmpString = "ftp://ftp.ncbi.nlm.nih.gov/" + path[1:len(path)-1] + "/" + bedFilename
				fmt.Printf("extracted full link >>> %s\n", tmpString)
				fullLinks = append(fullLinks, tmpString)
			}
		}
		tmpWgetConf = append(tmpWgetConf, fullLinks)
		wg.Done()
	}()
	wg.Add(1)
	// export plain BED files
	go func() {
		for _, url := range read(csvPath, 2) {
			path := extractPath(url)
			for _, bedFilename := range listBedFiles(serverConn, path) {
				tmpString = bedFilename[:len(bedFilename)-3]
				fmt.Printf("extracted file name >>> %s\n", tmpString)
				bedNames = append(bedNames, tmpString)
			}
		}
		tmpCMFT = append(tmpCMFT, bedNames)
		wg.Done()
	}()
	wg.Wait()
	fmt.Println("Done extracting BED filenames and URLs, disconnecting from ftp server")

	err = serverConn.Quit()
	checkError("Unable to disconnect from server: ", err)

	fmt.Println("Disconnected from server, writing files to current directory")

	file1, err := os.Create("cmft.tsv")
	checkError("error while exporting new cmft.csv: ", err)
	defer file1.Close()

	writer1 := csv.NewWriter(file1)
	defer writer1.Flush()

	file2, err := os.Create("wget.conf")
	checkError("error while exporting new cmft.csv: ", err)
	defer file2.Close()

	writer2 := csv.NewWriter(file2)
	defer writer2.Flush()

	tmpCMFT = transpose(tmpCMFT)
	tmpWgetConf = transpose(tmpWgetConf)
	wg.Add(1)
	go func() {
		for _, value := range tmpCMFT {
			err := writer1.Write(value)
			checkError("Cannot write to file: ", err)
		}
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		for _, value := range tmpWgetConf {
			err := writer2.Write(value)
			checkError("Cannot write to file: ", err)
		}
		wg.Done()
	}()
	wg.Wait()
	fmt.Println("All Done!")
}
