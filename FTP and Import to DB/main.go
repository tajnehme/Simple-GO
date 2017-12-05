package main

import (
	"golang.org/x/crypto/ssh"
	"log"
	"os"
	"github.com/pkg/sftp"
	"fmt"
	"time"
	"strings"
	"compress/gzip"
	"io"
	"bufio"
	"github.com/sadlil/go-trigger"
	"strconv"
	_"github.com/denisenkom/go-mssqldb"
	"database/sql"
	"flag"
	"sort"
)

var server = flag.String("server", "server", "the database server")
var port *int = flag.Int("port", "port", "the database port")
var user = flag.String("user", "username", "the database user")
var password = flag.String("password", "password", "the database password")


var myconnection *SQLConnection
var directory = "directory for ftp"
var path = "path to file"
var mapNUM map[int]NUMRecord
var columnsAvailable []string
var recordsImported int
//define constants to use in formatting the date
const (
	stdLongYear  = "2006"
	stdZeroMonth = "01"
	stdZeroDay   = "02"
)

type SQLConnection struct {
	originalSession *sql.DB
}
type NUMRecord struct {
	record map[string]string
}

func main(){
	recordsImported = 0

	fmt.Println("1-Cleaning folder..")
	CleanFolder()
	fmt.Println("2-Folder now Empty..")
	time.Sleep(time.Second*5)
	fmt.Println("3-Getting Files..")
	FTP_Get_HLRFiles()
	fmt.Println("4-Done Getting Files..")
	time.Sleep(time.Second*5)
	fmt.Println("5-Unzipping Files..")
	UNZIP()
	fmt.Println("6-Done Unzipping..")
	time.Sleep(time.Second*5)
	fmt.Println("6.5- Clearing Table")
	ClearTableInDB()
	time.Sleep(time.Second*5)
	fmt.Println("7-Read Files Into Map and Import to DB..")
	ReadFiles()
	FinalImport()


	fmt.Println("   ->Total Records Imported:   ",recordsImported)
}

// gets the current date in order to later on check if the filename contains that date, so that we get the correct current dump
func getCurrentDate()(todayDate string){
	t := time.Now()
	return t.Format(stdLongYear+stdZeroMonth+stdZeroDay)

}

func getPass() (string, error) {
	return "cnp200@HW", nil
}

func FTP_Get_HLRFiles(){

	sshConfig := &ssh.ClientConfig{
		User: "root",
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Auth: []ssh.AuthMethod{
			ssh.PasswordCallback(getPass),
		},
	}
	conn, err := ssh.Dial("tcp", "ftp:port", sshConfig)
	if err != nil {
		log.Println("Failed to Connect TCP: ",  err)
		return
	}
	sftp, serr := sftp.NewClient(conn)
	if serr != nil {
		log.Println("Failed to connect SFTP: ",  serr)
		conn.Close()
		return
	}
	remoteSourceDirectoryLength := len(directory)

	w := sftp.Walk(directory)

	//store path names into slice
	//var fileNames []string

	for w.Step() {

		if w.Err() != nil {
			continue
		}
		if w.Stat().IsDir() {
			directoryPath := path + w.Path()[remoteSourceDirectoryLength:]
			if err := os.MkdirAll(directoryPath, os.ModePerm); err != nil {
				log.Println("Failed to dial: ",  err)
				return
			}
		} else {
			//fmt.Println(strings.Contains(w.Path()[remoteSourceDirectoryLength:], getCurrentDate()))
			if(strings.Contains(w.Path()[remoteSourceDirectoryLength:], getCurrentDate())){
				//fileNames = append(fileNames, w.Path()[remoteSourceDirectoryLength:])
				filePath := path + w.Path()[remoteSourceDirectoryLength:]
				file, err := sftp.Open(w.Path())
				if err != nil {
					log.Println("Failed to open path: ",  err , filePath  )
					return
				}
				defer file.Close()
				outputFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
				if err != nil {
					log.Println("Failed to open file: ",  err)
					return
				}
				defer outputFile.Close()
				_, errw := file.WriteTo(outputFile)
				if errw != nil {
					log.Println("Failed to write files: ",  err)
					return
				}
			}

		}
	}

	conn.Close()

}

func UNZIP(){
	dir, err := os.Open(path)
	if err != nil {
		log.Println("Failed to open directory: ",  err   )
	}

	files, _ := dir.Readdirnames(0)

	for _,f := range files{
		fileToUnzip := path +f
		unzippedPath := path +f[0:len(f)-7] +".txt"
		if(!exists(unzippedPath)){

			f, err := os.Open(fileToUnzip)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()


			gr, err := gzip.NewReader(f)
			if err != nil {
				log.Fatal(err)
			}
			defer gr.Close()

			ff, _ := os.Create(unzippedPath)
			defer ff.Close()
			if _, err = io.Copy(ff, gr); err != nil {
				return
			}
			err = ff.Sync()
		}
	}

}


func exists(path string) (bool) {
	_, err := os.Stat(path)
	if err == nil { return true }
	if os.IsNotExist(err) { return false }
	return true
}

func CleanFolder(){

	dir, err := os.Open(path)
	if err != nil {
		log.Println("Failed to open directory: ",  err   )
	}

	files, _ := dir.Readdirnames(0)

	for _, f := range files {

		fileNamePath := path + f

		erRemove := removeFile(fileNamePath)
		if erRemove != nil {
			log.Println("Failed to remove path: ",  erRemove   )
			return
		}

	}
}


func removeFile(path string) error{


	er := os.Remove(path)
	if er != nil {
		log.Println("Failed to remove path: ",  er , path  )
		return nil
	}

	return nil
}

func ReadFiles(){
	mapNUM = make(map[int]NUMRecord)
	tr:=createTrigger()

	dir, err := os.Open(path)
	if err != nil {
		log.Println("Failed to open directory: ",  err   )
	}

	files, _ := dir.Readdirnames(0)

	for _, f := range files {

		file, err := os.Open(path+f)
		if err != nil {
			fmt.Println("Failed to Open file Path:", err)
		}
		defer file.Close()
		//var lines []string
		scanner := bufio.NewScanner(file)
		numFinal :=""
		counter := -1;
		for scanner.Scan() {
			//lines = append(lines, scanner.Text())
			//time.Sleep(time.Second*1)

			//fmt.Println(scanner.Text())
			result := AnalyzeLine(scanner.Text())

			//fmt.Println(result)

			switch result {
			// create empty num or initialize to empty again
			case "Begin":
				num,_ :=tr.Fire("firstEvent")
				numFinal = num[0].String()
				counter++
			// parse numFinal and sumbit to map
			case "End":
				numFinal = numFinal[:len(numFinal)-1]
				tr.Fire("secondEvent", numFinal,counter)
			// fill num fields into num string
			case "fill":
				res,_ := tr.Fire("thirdEvent",scanner.Text())
				numFinal += res[0].String()
			//ignore useless lines or errors that may occur
			case "other":
				tr.Fire("fourthEvent")

			}
			//fmt.Println(numFinal)
		}
		//fmt.Println(mapNUM)

	}

}

func createTrigger() (t trigger.Trigger){
	tr := trigger.New()
	tr.On("firstEvent",func()(num string) {
		num = ""
		return num

	})
	tr.On("secondEvent", func(numFinal string,key int) {
		//parses the record and calls a function to store it into the full map that has the key as the key
		parseNUM(numFinal)
	})
	tr.On("thirdEvent", func(line string)(res string) {
		res = line
		return res
	})
	tr.On("fourthEvent", func() {
		//fmt.Println("Ignore")
	})

	return tr
}

func AnalyzeLine(line string)(res string){
	if(strings.Contains(line, "SUBBEGIN")){
		return "Begin"
	}else if(strings.Contains(line, "SUBEND")){
		return "End"
	}else if(!strings.Contains(line, ";")){
		return "other"
	}else if(strings.Contains(line, ";")){
		return "fill"
	}
	return
}

func parseNUM(numFinal string) {
	//fmt.Println(numFinal)
	result := strings.Split(numFinal, ";")
	var _numRecord NUMRecord
	_numRecord = NUMRecord{
		record: make(map[string]string),
	}
	for _ ,res := range result{
		leftSide := strings.TrimSpace(res[:strings.Index(res, "=")])
		rightSide := strings.TrimSpace(res[strings.Index(res, "=")+1:])
		_numRecord.record[string(leftSide)]=string(rightSide)

	}
	//fmt.Println(_numRecord.record["key"])

	//Stores the num record into a full map with key as the key
	storeNUMIntoMap(_numRecord)


}

func storeNUMIntoMap(_numRecord NUMRecord){
	key,_ := strconv.Atoi(_numRecord.record["key"])
	mapNUM[key] = _numRecord
	if(len(mapNUM)%1024==0){
		fmt.Println("   ->Records Imported:   ",recordsImported)
		InsertIntoDB(mapNUM)
	}

}

func FinalImport(){
	InsertIntoDB(mapNUM)
}

func NewDBConnection() (conn *SQLConnection) {
	conn = new(SQLConnection)
	conn.createLocalConnection()
	return
}

func (c *SQLConnection) createLocalConnection() (err error) {
	//fmt.Println("Connecting to SQL DB server....")
	//SQL connection setup
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d", *server, *user, *password, *port)
	c.originalSession, err = sql.Open("mssql", connString)
	if err == nil {
		//fmt.Println("Connection established to SQL DB server")
		return
	} else {
		fmt.Printf("Error occured while creating SQL DB connection: %s", err.Error())
	}
	return
}

func InsertIntoDB(mapNUM  map[int]NUMRecord){

	myconnection := NewDBConnection()

	//get all available columns in database in case of future modifications
	columnsAvailable =myconnection.GetAllColumnsInTable()

	// loop over the map and check each number
	for key,num := range mapNUM{
		myconnection.DBInsertMap(num)
		delete(mapNUM, key)
	}

}

func (c *SQLConnection) DBInsertMap( num NUMRecord) (err error) {

	columns := make([]string,0)
	for key,_:=range num.record{
		columns =append(columns,key)
	}
	//fmt.Println(num)
	//check if all columns exist, if not alter the table and add the column
	for _,col:=range columns{

		if(!checkIfColumnExists(col)){
			lengthOfValue := len(num.record[col])+20
			strSql := "Alter table database add " +col+ " varchar("+strconv.Itoa(lengthOfValue)+") "
			queryResult, derr := c.originalSession.Query(strSql)
			defer queryResult.Close()
			if derr != nil {
				fmt.Println( "Error Creating New Column:  ", derr)
				return  derr
			}
			UpdateColumnsAvailable(col)
		}

	}
	keys :=""
	values :=""
	for kk,vv := range num.record{
		keys += kk +","
		values += "'"+vv+"',"
	}

	keys = keys[:len(keys)-1]
	values = values[:len(values)-1]
	//fmt.Println(num.record)

	strSql := " INSERT into database ("+keys+") "
	strSql += " Values("+values+")  "



	queryResult, derr := c.originalSession.Query(strSql)
	defer queryResult.Close()
	if derr != nil {
		//log.Println("DBInsertProcessedFiles Error: ", derr, "\n")
		return derr
	}
	recordsImported++
	return nil
}

func (c *SQLConnection) GetAllColumnsInTable() (columnsAvailabe []string) {

	strSql := "Select COLUMN_Name FROM database.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'table-name' "
	queryResult, rerr := c.originalSession.Query(strSql)
	defer queryResult.Close()
	if rerr != nil {
		return columnsAvailabe
	}
	for queryResult.Next() {
		var _column string
		err := queryResult.Scan(&_column)
		if err != nil {
			return columnsAvailabe
		}
		//add _temp to _NUM array
		columnsAvailabe = append(columnsAvailabe, _column)
	}
	return columnsAvailabe

}

func checkIfColumnExists(col string)(exists bool){
	//Sorting the string
	sort.Strings(columnsAvailable)
	i := sort.SearchStrings(columnsAvailable, col)
	return (i < len(columnsAvailable) && columnsAvailable[i] == col)
}

func UpdateColumnsAvailable(col string){
	columnsAvailable = append(columnsAvailable, col)

}

func ClearTableInDB(){
	myconnection := NewDBConnection()
	myconnection.DeleteTable()
}

func (c *SQLConnection) DeleteTable() (err error) {

	strSql := " delete database "

	queryResult, derr := c.originalSession.Query(strSql)
	defer queryResult.Close()
	if derr != nil {
		//log.Println("DBInsertProcessedFiles Error: ", derr, "\n")
		return derr
	}

	return nil
}