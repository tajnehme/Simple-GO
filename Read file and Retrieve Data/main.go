package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type data struct {
	size string
	source string
	count    int
}

// create var Data []data for each number
var completeMap map[string][]data
var highestCount map[string]data
var pathOut string

func main() {

	path := "C:/GoWorkspace/input.txt"
	pathOut = "C:/GoWorkspace/output.txt"
	completeMap = make(map[string][]data)
	highestCount = make(map[string]data)


	readFile(path)
	//fmt.Println(completeMap)
	getHighestCount()
	fmt.Println(len(completeMap))
	fmt.Println(len(highestCount))
	//fmt.Println(highestCount)


}

func readFile(path string) {

	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Failed to Open file Path:", err)
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	for i, line := range lines {
		if(i>1){
		//fmt.Println(line)
		number, _data := parseString(line)
		//fmt.Println(number, "-", _data)

		fillMap(number, _data )
		}

	}

}

func parseString(line string) (number string, _data data) {

	result := strings.Split(line, ",")
	number = strings.Trim(result[0], "\t ")
	size := strings.Trim(result[1], "\t ")
	source := strings.Trim(result[2], "\t ")
	count := strings.Trim(result[3], "\t ")

	countInt, _ := strconv.Atoi(count)
	_data = data{ size,source,countInt}

	return
}

func fillMap(number string, _data data){

	Data := completeMap[number]
	Data = append(Data, _data )
	completeMap[number] = Data
	//fmt.Println(completeMap)

}

func getHighestCount(){

	max:=data{"","", 0}
	for key,val := range completeMap{
		max.count = 0
		max.size =""
		for _, temp :=range val{
			diff:= max.count -temp.count
			if(diff<0){
				max.count = temp.count
				max.size = temp.size
				max.source =temp.source
			}
		}
		//fmt.Println(key, "", max)
		highestCount[key] = max

	}
	writeFile(pathOut)
}



func writeFile (path string ){

	f, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("error opening file: %v", err)
	}
	defer f.Close()
	for key,val := range highestCount{
		tempCount := strconv.Itoa(val.count)
		str := string(key +" , "+ tempCount +" , "+ val.size + "  ,  " +val.source)
		str += "\r\n"
		f.Write([]byte(str))
	}

}


