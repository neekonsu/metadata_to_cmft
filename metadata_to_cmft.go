package metadatatocmft

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	terminal "github.com/wayneashleyberry/terminal-dimensions"

	"github.com/jlaffaye/ftp"
)

// Isolate represents a 2d slice [][]string of cell mark file table data for one sample, hence the name Isolate(d sample)
type Isolate struct {
	table       [][]string
	controlName string
	sampleName  string
}

// Format individual Isolate with control assay filenames in fourth column and return formatted Isolate
func (I *Isolate) Format() {
	I.table = addColumn(I.table, makeColumn(I.controlName, len(I.table)))
}

// Takes []string and searches for element string, returns boolean
func sliceContains(input []string, element string) bool {
	for _, item := range input {
		if item == element {
			return true
		}
	}
	return false
}

// Iterate temporary (unformatted) cell mark file table 2d slice [][]string and return slice of Isolates []Isolate
func makeIsolates(input [][]string) []Isolate {
	var output []Isolate
	for _, row := range input {
		var isolate Isolate
		isolate.sampleName = row[0]
		existsInOutput := false
		indexOfIsolate := 0
		for i := range output {
			if output[i].sampleName == isolate.sampleName {
				output[i].table = append(output[i].table, row)
				existsInOutput = true
				indexOfIsolate = i
			}
		}
		if !existsInOutput {
			if row[1] == "ChIP-Seq input" {
				isolate.controlName = row[2]
			}
			isolate.table = append(isolate.table, row)
			output = append(output, isolate)
		} else if row[1] == "ChIP-Seq input" {
			output[indexOfIsolate].controlName = row[2]
		}
	}
	return output
}

// Take temporary cell mark file table, convert to []Isolate, iterate Isolates in []Isolate and format each one, iterate []Isolate and append each table to a new finished cell mark file table
func formatCMFT(input [][]string, purge bool) [][]string {
	var output [][]string
	isolates := makeIsolates(input)
	for _, isolate := range isolates {
		isolate.Format()
		output = append(output, isolate.table[:]...)
	}
	if purge {
		for i := 0; i < len(output); i++ {
			row := output[i]
			if row[3] == "" {
				output = append(output[:i], output[i+1:]...)
				i--
			}
		}
	}
	return output
}

// Take string and generate []string of set length containing that string
func makeColumn(input string, length int) []string {
	var output []string
	for i := 0; i < length; i++ {
		output = append(output, input)
	}
	return output
}

// Take []string and append as n+1st column of 2d slice [][]string
func addColumn(input [][]string, newColumn []string) [][]string {
	var output [][]string = input
	for i := range input {
		output[i] = append(output[i], newColumn[i])
	}
	return output
}

// Loop 2d slice and return specified column as []string
func getColumn(input [][]string, index int) []string {
	var output []string
	for _, row := range input {
		output = append(output, row[index])
	}
	return output
}

// Read entries from one column of a csv to []string
func read(path string, index int8) []string {
	csvfile, _ := os.Open(path)
	r := csv.NewReader(csvfile)
	r.Read()
	var output []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		checkError("Unable to read line from csv: ", err)
		output = append(output, record[index])
	}
	return output
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
	// declare variables
	var inputPath string = *flag.String("i", "./metadata.csv", "Path to metadata csv file, default is \"./metadata.csv\"")
	var cmftOutputPath string = *flag.String("o", "./cmft.tsv", "Path to cmft output, default \"./cmft.tsv\"")
	var wgetOutputPath string = *flag.String("wgetPath", filepath.Dir(cmftOutputPath)+"/wget.conf", "Path to wget dependency file, default /path/to/cmft/wget.conf")
	var purge bool = *flag.Bool("purge", false, "T/F purge samples with missing control sequences, default \"false\"")
	var tmpString string
	var fullLinks []string
	var bedNames []string
	var cmft [][]string
	var wgetConf [][]string

	// initialize variables that are able to be assigned values at this point
	termWidth, _ := terminal.Width()
	sampleNames := read(inputPath, 1)
	marks := read(inputPath, 3)

	// initialize connection to ftp server (serverConn)
	serverConn, err := ftp.Dial("ftp.ncbi.nlm.nih.gov:21")
	checkError("Unable to dial ftp server: ", err)
	err = serverConn.Login("anonymous", "anonymous")
	checkError("Unable to authenticate ftp server: ", err)
	fmt.Println("ftp server connected!")
	fmt.Println("extracting BED filenames and URLs")

	// export full links and BED filenames
	for _, url := range read(inputPath, 2) {
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

	// disconnect from the server and provide verbose
	fmt.Printf("%s\n", strings.Repeat("~", int(termWidth*3/4)))
	fmt.Println("Done extracting BED filenames and URLs, disconnecting from ftp server")
	err = serverConn.Quit()
	checkError("Unable to disconnect from server: ", err)
	fmt.Println("Disconnected from server, writing files to current directory")

	// create placeholder files for cmft.tsv and wget.conf
	file1, err := os.Create(cmftOutputPath)
	checkError("error while exporting new cmft.tsv: ", err)
	defer file1.Close()
	file2, err := os.Create(wgetOutputPath)
	checkError("error while exporting new wget.csv: ", err)
	defer file2.Close()

	// create custom file writers that will write our files in the tsv format
	writer1 := csv.NewWriter(file1)
	writer1.Comma = '\t'
	defer writer1.Flush()
	writer2 := csv.NewWriter(file2)
	writer2.Comma = '\t'
	defer writer2.Flush()

	// append []string of full ftp addresses to wgetConf
	// wgetConf is [][]string so that it can be transposed
	// by transposing wgetConf, we ultimately write each link as one row in wget.conf
	wgetConf = append(wgetConf, fullLinks)
	wgetConf = transpose(wgetConf)

	// now that we have all necessary information to make an unformatted cmft
	// we add all of the necessary columns to cmft as rows
	// we then transpose cmft one time
	// if we had used addColumn() instead of append() and transpose(),
	// we would have effectively transposed each column individually,
	// which would constitute redundant computations, therefore I chose
	// to first append(the columns as rows) then transpose(cmft)
	cmft = append(cmft, sampleNames)
	cmft = append(cmft, marks)
	cmft = append(cmft, bedNames)
	cmft = transpose(cmft)

	// format cmft with flag options
	cmft = formatCMFT(cmft, purge)

	// write the contents of cmft and wgetConf to their respective files row by row
	for _, value := range cmft {
		err := writer1.Write(value)
		checkError("Cannot write to file: ", err)
	}
	for _, value := range wgetConf {
		err := writer2.Write(value)
		checkError("Cannot write to file: ", err)
	}

	fmt.Println("All Done!")

}
