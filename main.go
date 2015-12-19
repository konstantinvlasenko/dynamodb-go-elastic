package main
import (
    "os"
    "fmt"
    "bytes"
    "net/http"
    "io/ioutil"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
)
func main() {
    svc := dynamodb.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})

    tableName := os.Args[2]
    params := &dynamodb.ScanInput{ TableName: aws.String(tableName) }
    resp, err := svc.Scan(params)

    if err != nil {
        fmt.Println(err.Error())
        return
    }

    idName := os.Args[3]
    bulk := ""
    for _,record := range resp.Items {
        document := `{ "index" : { "_id" : "` + *record[idName].S  + "\" } }\n{"
        for field, v := range record {
             stringValue := v.S
             numericValue := v.N
             if(stringValue != nil) {
               document += ` "` + field + `" : "` + *stringValue + `",`
             }
             if(numericValue != nil){
               document += ` "` + field + `" : ` + *numericValue + `,`
             }
        }
        size := len(document)
        bulk += document[:size-1] + " }\n"
    }
    bulkIndex([]byte(bulk))
}

func bulkIndex(json []byte) {
    url := os.Args[1]
    resp, err := http.Post(url, "application/json", bytes.NewBuffer(json))
    if err != nil {
        fmt.Println(err.Error())
        return
    }
    defer resp.Body.Close()
    body, _ := ioutil.ReadAll(resp.Body)
    fmt.Println(string(body))
}
