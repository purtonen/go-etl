package main

import (
	"database/sql"
	"fmt"
	"iter"
	"reflect"
	"strings"
	"unsafe"

	"github.com/blockloop/scan"
	"github.com/fatih/structs"
	_ "github.com/lib/pq"
)

var db *sql.DB

const (
	host     = "localhost"
	port     = 5432
	user     = "user"
	password = "password"
	dbname   = "goetl"
)

type Entity struct {
	Field       string  `db:"field1"`
	Numberfield float64 `db:"numberfield1"`
}

// This function will make a connection to the database only once.
func init() {
	var err error

	connStr := "postgres://user:password@localhost/goetl?sslmode=disable"
	db, err = sql.Open("postgres", connStr)

	if err != nil {
		panic(err)
	}

	if err = db.Ping(); err != nil {
		panic(err)
	}
	// this will be printed in the terminal, confirming the connection to the database
	fmt.Println("The database is connected")
}

func MapIterator[T, U any](seq iter.Seq[T], f func(T) U) iter.Seq[U] {
	return func(yield func(U) bool) {
		for a := range seq {
			if !yield(f(a)) {
				return
			}
		}
	}
}

// Map takes two slice 's' as source and 'd' as destination
// and a map function, then applies it to each element of 's' and
// store the output in the current index of 'd'.
func Map[In any, Out any](input []In, mapFunc func(In) Out) []Out {
	var output []Out = []Out{}
	for _, item := range input {
		output = append(output, mapFunc(item))
	}
	return output
}

func getFieldColumnName(field *structs.Field) string {
	return field.Tag("col")
}

func getPointerToField(field *structs.Field, rv reflect.Value) unsafe.Pointer {
	fieldName := field.Name()
	fieldValue := rv.FieldByName(fieldName)
	return fieldValue.UnsafePointer()
}

func main() {
	// connection string
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)

	// close database
	defer db.Close()

	// check db
	err = db.Ping()
	CheckError(err)

	fmt.Println("Connected!")

	cols := Map(structs.Fields(&Entity{}), getFieldColumnName)
	rows, err := db.Query(fmt.Sprintf("SELECT %s from table1 limit 1", strings.Join(cols, ",")))
	CheckError(err)

	defer rows.Close()

	var entities []Entity
	err = scan.Rows(&entities, rows)
	CheckError(err)

	fmt.Printf("%#v", entities)
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}
