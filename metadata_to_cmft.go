package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/jlaffaye/ftp"
	terminal "github.com/wayneashleyberry/terminal-dimensions"
)

// Contains a 2d slice [][]string of cell mark file table data for one sample, hence the name isolate(d sample)
type isolate struct {
	table [][]string
}

// Format individual isolate with control assay filenames in fourth column and return formatted isolate
func (I *isolate) Format() {

}

// Iterate temporary (unformatted) cell mark file table 2d slice [][]string and return slice of isolates []isolate
func makeIsolates(unformattedCMFT [][]string) {

}

// Take temporary cell mark file table, convert to []isolate, iterate isolates in []isolate and format each one, iterate []isolate and append each table to a new finished cell mark file table
func formatCMFT(unformattedCMFT [][]string) {

}

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
		outTable = append(outTable, record[index])
	}
	return outTable
}

func readAll(path string) [][]string {
	csvfile, _ := os.Open(path)
	r := csv.NewReader(csvfile)
	r.Read()
	var outTable [][]string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		checkError("Unable to read line from csv: ", err)
		outTable = append(outTable, record)
	}
	return outTable
}

// TODO: Generalize to any base domain/ftp server
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

	termWidth, _ := terminal.Width()
	sampleNames := read(csvPath, 1)
	marks := read(csvPath, 3)
	tmpCMFT = append(tmpCMFT, sampleNames)
	tmpCMFT = append(tmpCMFT, marks)

	serverConn, err := ftp.Dial("ftp.ncbi.nlm.nih.gov:21")
	checkError("Unable to dial ftp server: ", err)

	err = serverConn.Login("anonymous", "anonymous")
	checkError("Unable to authenticate ftp server: ", err)

	fmt.Println("ftp server connected!")
	fmt.Println("extracting BED filenames and URLs")

	// export full links and BED filenames
	for _, url := range read(csvPath, 2) {
		path := extractPath(url)
		for _, bedFilename := range listBedFiles(serverConn, path) {
			tmpString = "ftp://ftp.ncbi.nlm.nih.gov/" + path[1:len(path)-1] + "/" + bedFilename
			fmt.Printf("%s\n", strings.Repeat("~", int(termWidth*3/4)))
			fmt.Printf(" <<< extracted full link >>> %s\n", tmpString)
			fmt.Printf(" <<< extracted file name >>> %s\n", bedFilename[:len(bedFilename)-3])
			fullLinks = append(fullLinks, tmpString)
			bedNames = append(bedNames, bedFilename[:len(bedFilename)-3])
		}
	}
	tmpWgetConf = append(tmpWgetConf, fullLinks)
	tmpCMFT = append(tmpCMFT, bedNames)

	fmt.Printf("%s\n", strings.Repeat("~", int(termWidth*3/4)))
	fmt.Println("Done extracting BED filenames and URLs, disconnecting from ftp server")

	err = serverConn.Quit()
	checkError("Unable to disconnect from server: ", err)
	fmt.Println("Disconnected from server, writing files to current directory")

	file1, err := os.Create("cmft.tsv")
	checkError("error while exporting new cmft.tsv: ", err)
	defer file1.Close()

	file2, err := os.Create("wget.conf")
	checkError("error while exporting new wget.csv: ", err)
	defer file2.Close()

	writer1 := csv.NewWriter(file1)
	writer1.Comma = '\t'
	defer writer1.Flush()

	writer2 := csv.NewWriter(file2)
	writer2.Comma = '\t'
	defer writer2.Flush()

	tmpCMFT = transpose(tmpCMFT)
	tmpWgetConf = transpose(tmpWgetConf)

	for _, value := range tmpCMFT {
		err := writer1.Write(value)
		checkError("Cannot write to file: ", err)
	}

	for _, value := range tmpWgetConf {
		err := writer2.Write(value)
		checkError("Cannot write to file: ", err)
	}

	fmt.Println("All Done!")
}
