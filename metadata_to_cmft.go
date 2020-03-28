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
	terminal "github.com/wayneashleyberry/terminal-dimensions"
)

// same as [][]string, meant to represent tables containing only one tissue sample
type isolate struct {
	table [][]string
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

func matrixToIsolate(matrix [][]string) isolate {
	var i isolate
	i.table = matrix
	return i
}

// Convert raw cmft to []isolate
func isolates(rawCMFT [][]string) []isolate {
	// TODO
	var output []isolate

	for _, i := range rawCMFT {
		var tmpIsolate [][]string
		tmpSample := i[0]
		for ii := 0; ii < len(rawCMFT); ii++ {
			if rawCMFT[ii][0] == tmpSample {
				tmpIsolate = append(tmpIsolate, rawCMFT[ii])
				rawCMFT = append(rawCMFT[:ii], rawCMFT[ii+1:]...)
				ii--
			}
		}
		output = append(output, matrixToIsolate(tmpIsolate))
	}

	return output
}

// Takes individual raw isolate and returns properly formatted isolate
func (e *isolate) linkControlAssays() {
	// example raw isolate:
	// {
	// 		{SAMN00012131,	H3K9me3,	GSM537695_BI.Adult_Liver.H3K9me3.3.bed},
	// 		{SAMN00012131,	H3K4me3,	GSM537697_BI.Adult_Liver.H3K4me3.3.bed},
	// 		{SAMN00012131,	H3K27me3,	GSM537698_BI.Adult_Liver.H3K27me3.3.bed},
	// 		{SAMN00012131,	CHIp-seq Input,	GSM537698_BI.Adult_Liver.input.3.bed}
	// }

	// init vars
	var controlFilename string
	var matrixLenY int
	var controlFilenameIndex int

	// assign values to matrix dimensions
	matrixLenY = len(e.table)

	// search for control filename and assign to variable
	for i := 0; i < matrixLenY; i++ {
		if e.table[i][1] == "ChIP-seq Input" {
			controlFilename = e.table[i][2]
			controlFilenameIndex = i
		}
	}

	// remove row containing Control assay
	e.table = append(e.table[:controlFilenameIndex], e.table[controlFilenameIndex+1:]...)

	// append control assay filename to all rows
	for i := 0; i < matrixLenY; i++ {
		e.table[i] = append(e.table[i], controlFilename)
	}

	// example formatted isolate:
	// {
	// 		{SAMN00012131,	H3K9me3,	GSM537695_BI.Adult_Liver.H3K9me3.3.bed,	GSM537698_BI.Adult_Liver.input.3.bed},
	// 		{SAMN00012131,	H3K4me3,	GSM537697_BI.Adult_Liver.H3K4me3.3.bed,	GSM537698_BI.Adult_Liver.input.3.bed},
	// 		{SAMN00012131,	H3K27me3,	GSM537698_BI.Adult_Liver.H3K27me3.3.bed,	GSM537698_BI.Adult_Liver.input.3.bed}
	// }
}

func (e isolate) toMatrix() [][]string {
	return e.table
}

func formatCMFT(rawCMFT [][]string) [][]string {
	var output [][]string
	rawIsolates := isolates(rawCMFT)
	for i := 0; i < len(rawIsolates); i++ {
		rawIsolates[i].linkControlAssays()
		output = append(output, rawIsolates[i].toMatrix()...)
	}
	return output
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

	serverConn, err := ftp.Dial("ftp.ncbi.nlm.nih.gov:21", ftp.DialWithTimeout(5*time.Second))
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

	file1, err := os.Create("cmft.csv")
	checkError("error while exporting new cmft.csv: ", err)
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
