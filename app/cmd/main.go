/*****************************************************************************
*
*	File			: main.go
*
* 	Created			: 4 Dec 2023
*
*	Description		: Golang Fake data producer, part of the MongoCreator project.
*					: We will create a sales basket of items as a document, to be posted onto Confluent Kafka topic, we will then
*					: then seperately post a payment onto a seperate topic. Both topics will then be sinked into MongoAtlas collections
*					: ... EXPAND ...
*
*	Modified		: 12 July 2024
*					: This is fork from the ProtoBuf version of the project into a Avro based version.
*
*	Git				: https://github.com/georgelza/MongoCreator-GoProducer-avro.git
*
*	Author			: George Leonard
*
*	Copyright © 2021: George Leonard georgelza@gmail.com aka georgelza on Discord and Mongo Community Forum
*
*	jsonformatter 	: https://jsonformatter.curiousconcept.com/#
*					: https://jsonlint.com/
*
*  	json to avro schema	: http://www.dataedu.ca/avro
*
*****************************************************************************/

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"

	"github.com/google/uuid"

	"github.com/TylerBrock/colorjson"

	"github.com/tkanos/gonfig"
	glog "google.golang.org/grpc/grpclog"

	// My Types/Structs/functions
	"cmd/types"

	// Filter JSON array
	// MongoDB
	//
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	// Mysql
	mysql "database/sql"

	_ "github.com/go-sql-driver/mysql"

	// Postgres
	pgsql "database/sql"

	_ "github.com/lib/pq"
)

type tMSPayment types.TPPayment // MySql
type tMSPayments []types.TPPayment
type tPGPayment types.TPPayment // postgreSql
type tPGPayments []types.TPPayment
type tGeneral types.TPGeneral
type tKafka types.TPKafka
type tMongodb types.TPMongodb
type tMysqldb types.TPMysqldb
type tPgsqldb types.TPPgsqldb
type tSeed types.TPSeed

var (
	grpcLog glog.LoggerV2

	vSeed    tSeed
	vGeneral tGeneral
	vKafka   tKafka
	vMongodb tMongodb
	vMysqldb tMysqldb
	vPgsqldb tPgsqldb

	pathSep = string(os.PathSeparator)
	runId   string
)

func init() {

	// Keeping it very simple
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

	grpcLog.Infoln("###############################################################")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   Project   : GoProducer 4.0 - Avro based")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   Comment   : MongoCreator Project and lots of Kafka")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   By        : George Leonard (georgelza@gmail.com)")
	grpcLog.Infoln("#")
	grpcLog.Infoln("#   Date/Time :", time.Now().Format("2006-01-02 15:04:05"))
	grpcLog.Infoln("#")
	grpcLog.Infoln("###############################################################")
	grpcLog.Infoln("")
	grpcLog.Infoln("")

}

func (vGeneral tGeneral) load(params ...string) tGeneral {

	var err error

	//	vGeneral := types.TPGeneral{}
	env := "dev"
	if len(params) > 0 { // Input environment was specified, so lets use it
		env = params[0]
		grpcLog.Info("*")
		grpcLog.Infof("* Called with Argument\t\t\t%s", env)
		grpcLog.Info("*")

	}

	vGeneral.CurrentPath, err = os.Getwd()
	if err != nil {
		grpcLog.Fatalf("Problem retrieving current path: %s\n", err)

	}
	vGeneral.ConfigPath = fmt.Sprintf("%s%sconf", vGeneral.CurrentPath, pathSep)
	vGeneral.OSName = runtime.GOOS

	// General config file
	vGeneral.AppConfigFile = fmt.Sprintf("%s%s%s_app.json", vGeneral.ConfigPath, pathSep, env)
	err = gonfig.GetConf(vGeneral.AppConfigFile, &vGeneral)
	if err != nil {
		grpcLog.Fatalf("Error Reading Config File: %s\n", err)

	} else {
		vHostname, err := os.Hostname()
		if err != nil {
			grpcLog.Fatalf("Can't retrieve hostname %s\n", err)

		}
		vGeneral.Hostname = vHostname
		vGeneral.SeedConfigFile = fmt.Sprintf("%s%s%s", vGeneral.ConfigPath, pathSep, vGeneral.SeedFile)

	}

	if vGeneral.Json_to_file == 1 {
		vGeneral.Output_path = fmt.Sprintf("%s%s%s", vGeneral.CurrentPath, pathSep, vGeneral.Output_path)
	}
	return vGeneral
}

// Load Kafka specific configuration Parameters, this is so that we can gitignore this dev_kafka.json file/seperate
// from the dev_app.json file
func (vKafka tKafka) load(vGeneral tGeneral, params ...string) tKafka {

	env := "dev"
	if len(params) > 0 {
		env = params[0]
	}

	vKafka.KafkaConfigFile = fmt.Sprintf("%s%s%s_kafka.json", vGeneral.ConfigPath, pathSep, env)
	err := gonfig.GetConf(vKafka.KafkaConfigFile, &vKafka)
	if err != nil {
		grpcLog.Fatalf("Error Reading Kafka File: %s\n", err)

	}
	vKafka.Sasl_password = os.Getenv("SASL_PASSWORD")
	vKafka.Sasl_username = os.Getenv("SASL_USERNAME")

	return vKafka
}

func (vMongodb tMongodb) load(vGeneral tGeneral, params ...string) tMongodb {

	env := "dev"
	if len(params) > 0 {
		env = params[0]
	}

	vMongodb.MongoConfigFile = fmt.Sprintf("%s%s%s_mongo.json", vGeneral.ConfigPath, pathSep, env)
	err := gonfig.GetConf(vMongodb.MongoConfigFile, &vMongodb)
	if err != nil {
		grpcLog.Fatalf("Error Reading Mongo File: %s\n", err)

	}

	vMongodb.Username = os.Getenv("MONGO_USERNAME")
	vMongodb.Password = os.Getenv("MONGO_PASSWORD")

	if vMongodb.Username != "" {
		vMongodb.Uri = fmt.Sprintf("%s://%s:%s@%s/", vMongodb.Root, vMongodb.Username, vMongodb.Password, vMongodb.Url)

	} else {
		vMongodb.Uri = fmt.Sprintf("%s://%s&w=majority", vMongodb.Root, vMongodb.Url)
	}

	return vMongodb
}

func (vMysqldb tMysqldb) load(vGeneral tGeneral, params ...string) tMysqldb {

	env := "dev"
	if len(params) > 0 {
		env = params[0]
	}

	vMysqldb.MysqlConfigFile = fmt.Sprintf("%s%s%s_mysql.json", vGeneral.ConfigPath, pathSep, env)
	err := gonfig.GetConf(vMysqldb.MysqlConfigFile, &vMysqldb)
	if err != nil {
		grpcLog.Fatalf("Error Reading Mysql File: %s\n", err)

	}

	vMysqldb.Username = os.Getenv("MYSQL_ROOT_USER")
	vMysqldb.Password = os.Getenv("MYSQL_ROOT_PASSWORD")

	vMysqldb.Dsn = vMysqldb.DSN()

	return vMysqldb
}

func (vPgsqldb tPgsqldb) load(vGeneral tGeneral, params ...string) tPgsqldb {

	env := "dev"
	if len(params) > 0 {
		env = params[0]
	}

	vPgsqldb.PgsqlConfigFile = fmt.Sprintf("%s%s%s_pgsql.json", vGeneral.ConfigPath, pathSep, env)
	err := gonfig.GetConf(vPgsqldb.PgsqlConfigFile, &vPgsqldb)
	if err != nil {
		grpcLog.Fatalf("Error Reading Pgsql File: %s\n", err)

	}

	vPgsqldb.Username = os.Getenv("POSTGRES_USER")
	vPgsqldb.Password = os.Getenv("POSTGRES_PASSWORD")

	vPgsqldb.Url = vPgsqldb.URL()

	return vPgsqldb
}

func (vSeed tSeed) load(fileName string) tSeed {

	err := gonfig.GetConf(fileName, &vSeed)
	if err != nil {
		grpcLog.Fatalf("Error Reading Seed File: %s\n", err)

	}

	v, err := json.Marshal(vSeed)
	if err != nil {
		grpcLog.Fatalf("Marchalling error: %s\n", err)
	}

	if vGeneral.EchoSeed == 1 {
		prettyJSON(string(v))

	}

	return vSeed
}

// func (xGeneral tGeneral) Config(vGeneral types.TPGeneral) (err error) {
func (xGeneral tGeneral) Config() {

	grpcLog.Info("************** General Parameters ****************************")
	grpcLog.Info("*")
	grpcLog.Info("* Hostname is\t\t\t\t\t", xGeneral.Hostname)
	grpcLog.Info("* OS is \t\t\t\t\t", xGeneral.OSName)
	grpcLog.Info("*")
	grpcLog.Info("* Debug Level is\t\t\t\t", xGeneral.Debuglevel)
	grpcLog.Info("*")
	grpcLog.Info("* Sleep Duration is\t\t\t\t", xGeneral.Sleep, " milliseeconds")
	grpcLog.Info("* Test Batch Size is\t\t\t\t", xGeneral.Testsize, " records")
	grpcLog.Info("* Echo Seed is\t\t\t\t", xGeneral.EchoSeed)
	grpcLog.Info("* Json to File is\t\t\t\t", xGeneral.Json_to_file)
	if xGeneral.Json_to_file == 1 {
		grpcLog.Infoln("* Output path\t\t\t\t\t", xGeneral.Output_path)
	}

	grpcLog.Info("* Kafka Enabled is\t\t\t\t", xGeneral.KafkaEnabled)
	grpcLog.Info("* Mongo Enabled is\t\t\t\t", xGeneral.MongoAtlasEnabled)
	grpcLog.Info("* Mysql Enabled is\t\t\t\t", xGeneral.MysqlEnabled)

	grpcLog.Info("* App Path is\t\t\t\t\t", xGeneral.CurrentPath)
	grpcLog.Info("* App Config File is\t\t\t\t", xGeneral.AppConfigFile)
	grpcLog.Info("* Seed Config File is\t\t\t\t", xGeneral.SeedConfigFile)
	grpcLog.Info("*")
	grpcLog.Info("**************************************************************")

	grpcLog.Info("")

}

// print some more configurations
// func printKafkaConfig(vKafka types.TPKafka) {
func (xKafka tKafka) Config() {

	grpcLog.Info("************** Kafka Connection Parameters *******************")
	grpcLog.Info("*")
	grpcLog.Info("* Kafka Config file is\t\t\t", xKafka.KafkaConfigFile)
	grpcLog.Info("* Kafka bootstrap Server is\t\t\t", xKafka.Bootstrapservers)
	grpcLog.Info("* Kafka schema Registry is\t\t\t", xKafka.SchemaRegistryURL)
	grpcLog.Info("* Kafka Basket Topic is\t\t\t", xKafka.BasketTopicname)
	grpcLog.Info("* Kafka Payment Topic is\t\t\t", xKafka.PaymentTopicname)
	grpcLog.Info("* Kafka # Partitions is\t\t\t", xKafka.Numpartitions)
	grpcLog.Info("* Kafka Rep Factor is\t\t\t\t", xKafka.Replicationfactor)
	grpcLog.Info("* Kafka Retension is\t\t\t\t", xKafka.Retension, " minutes")
	grpcLog.Info("* Kafka ParseDuration is\t\t\t", xKafka.Parseduration)
	grpcLog.Info("* Kafka SASL Mechanism is\t\t\t", xKafka.Sasl_mechanisms)
	grpcLog.Info("* Kafka SASL Username is\t\t\t", xKafka.Sasl_username)
	grpcLog.Info("* Kafka Flush Size is\t\t\t\t", xKafka.Flush_interval, " records")
	grpcLog.Info("* Kafka Flush Timeout is\t\t\t", xKafka.Flush_timeout, " milliseconds")
	grpcLog.Info("*")
	grpcLog.Info("**************************************************************")

	grpcLog.Info("")

}

// print some more configurations
func (xMongo tMongodb) Config() {

	grpcLog.Info("*")
	grpcLog.Info("************** MongoDB Connection Parameters *****************")
	grpcLog.Info("*")
	grpcLog.Info("* Mongo Config file is\t\t\t", xMongo.MongoConfigFile)
	grpcLog.Info("* Mongo URL is\t\t\t\t", xMongo.Url)
	grpcLog.Info("* Mongo Port is\t\t\t\t", xMongo.Port)
	grpcLog.Info("* Mongo DataStore is\t\t\t\t", xMongo.Datastore)
	grpcLog.Info("* Mongo Username is\t\t\t\t", xMongo.Username)
	grpcLog.Info("* Mongo Basket Collection is\t\t\t", xMongo.Basketcollection)
	grpcLog.Info("* Mongo Payment Collection is\t\t\t", xMongo.Paymentcollection)
	grpcLog.Info("* Mongo Batch size is\t\t\t\t", xMongo.Batch_size, " record/s")
	grpcLog.Info("*")
	grpcLog.Info("**************************************************************")

	grpcLog.Info("")

}

// print some more configurations
func (xMysqldb tMysqldb) Config() {

	grpcLog.Info("*")
	grpcLog.Info("************** MySqlDB Connection Parameters *****************")
	grpcLog.Info("*")
	grpcLog.Info("* MySqlDB Config file is\t\t\t", xMysqldb.MysqlConfigFile)
	grpcLog.Info("* MySqlDB DSN is\t\t\t\t", xMysqldb.Dsn)
	grpcLog.Info("* MySqlDB DataStore is\t\t\t", xMysqldb.Dbname)
	grpcLog.Info("* MySqlDB Username is\t\t\t\t", xMysqldb.Username)
	grpcLog.Info("* MySqlDB Payment Table is\t\t\t", xMysqldb.PaymentTable)
	grpcLog.Info("* MySqlDB Batch size is\t\t\t", xMysqldb.Batch_size, " records/s")
	grpcLog.Info("*")
	grpcLog.Info("**************************************************************")

	grpcLog.Info("")

}

// print some more configurations
func (xPgsqldb tPgsqldb) Config() {

	grpcLog.Info("*")
	grpcLog.Info("************** PgSqlDB Connection Parameters *****************")
	grpcLog.Info("*")
	grpcLog.Info("* PgSqlDB Config file is\t\t\t", xPgsqldb.PgsqlConfigFile)
	grpcLog.Info("* PgSqlDB URL is\t\t\t\t", xPgsqldb.Url)
	grpcLog.Info("* PgSqlDB Database is\t\t\t\t", xPgsqldb.Dbname)
	grpcLog.Info("* PgSqlDB Username is\t\t\t\t", xPgsqldb.Username)
	grpcLog.Info("* PgSqlDB Schema is\t\t\t\t", xPgsqldb.Schema)
	grpcLog.Info("* PgSqlDB Payment Table is\t\t\t", xPgsqldb.PaymentTable)
	grpcLog.Info("* PgSqlDB Batch size is\t\t\t", xPgsqldb.Batch_size, " record/s")
	grpcLog.Info("*")
	grpcLog.Info("**************************************************************")

	grpcLog.Info("")

}

// Some Helper Functions

// MongoDB Connection Creator
func (xMongodb tMongodb) Connection(vMongodb tMongodb) (mongoDB *mongo.Database, err error) {

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)

	opts := options.Client().ApplyURI(vMongodb.Uri).SetServerAPIOptions(serverAPI)

	grpcLog.Infof("* dbMongoDBConnection: MongoDB URI Constructed: %s\n", vMongodb.Uri)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	grpcLog.Infoln("* dbMongoDBConnection: Succesffully Created MongoDB Context Object")
	defer cancel()

	Mongoclient, err := mongo.Connect(ctx, opts)
	if err != nil {
		grpcLog.Errorf("* dbMongoDBConnection: MongoDB Connection Failed: %s\n", err)
		return nil, err
	}
	grpcLog.Infoln("* dbMongoDBConnection: Successfully confgured MongoDB Client Connection")

	// Ping the primary
	if err = Mongoclient.Ping(ctx, readpref.Primary()); err != nil {
		grpcLog.Errorf("* dbMongoDBConnection: There was a error creating the MongoDBClient object, Ping failed: %s", err)
		return nil, err

	}
	grpcLog.Infoln("* dbMongoDBConnection: Successfully Pinged MongoDB Client")

	// Define the Mongo Datastore
	mongoDB = Mongoclient.Database(vMongodb.Datastore)

	if vGeneral.Debuglevel > 0 {
		grpcLog.Infoln("* dbMongoDBConnection: Successfully Intialized MongoDB Datastore and Collections")
		grpcLog.Infoln("*")
	}

	return mongoDB, nil
}

// MysqlDB DSN Builder
func (vMysqldb tMysqldb) DSN() string {

	return fmt.Sprintf("%s:%s@tcp(%s)/%s",
		vMysqldb.Username,
		vMysqldb.Password,
		vMysqldb.Hostname,
		vMysqldb.Dbname,
	)
}

func (vMysqldb tMysqldb) Connection() (msdb *mysql.DB, err error) {

	// MysqlDB Connection Creator
	//func connectMysqlDB(vMysqldb tMysqldb) (msdb *mysql.DB, err error) {

	// https://golangbot.com/connect-create-db-mysql/
	//
	// We've created the database above so lets reconnect now to created database
	msdb, err = mysql.Open("mysql", vMysqldb.DSN())
	if err != nil {
		grpcLog.Errorf("* connectMysqlDB: Error %s when opening MySqlDB\n", err)
		return nil, err
	}
	grpcLog.Infoln("* connectMysqlDB: Succesffully Opened MySqlDB Connection")

	msdb.SetMaxOpenConns(20)
	msdb.SetMaxIdleConns(20)
	msdb.SetConnMaxLifetime(time.Minute * 5)

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	err = msdb.PingContext(ctx)
	if err != nil {
		grpcLog.Errorf("* connectMysqlDB: Errors %s pinging MySqlDB\n", err)
		return nil, err
	}
	grpcLog.Infoln("* connectMysqlDB: Succesffully Pinged MySqlDB Connection")

	grpcLog.Infof("* connectMysqlDB: Successfully Connected to MySqlDB: (%s)\n", vMysqldb.Dbname)

	return msdb, nil
}

// Mysql Single Inserter
func (pmnt tMSPayment) SingleInsert(msdb *mysql.DB) (err error) {

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()

	sqlStatement := "INSERT INTO salespayments(invoiceNumber, payDateTime_Ltz, payTimestamp_Epoc, paid, finTransactionId) VALUES (?, ?, ?, ?, ?)"
	stmt, err := msdb.PrepareContext(ctx, sqlStatement)
	if err != nil {
		grpcLog.Errorf("MySql SingleInsert: Error %s when preparing SQL statement: `%s`\n", err, sqlStatement)
		return err
	}

	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, pmnt.InvoiceNumber, pmnt.PayDateTime_Ltz, pmnt.PayTimestamp_Epoc, pmnt.Paid, pmnt.FinTransactionID)
	if err != nil {
		grpcLog.Errorf("MySql SingleInsert: Error %s when inserting row into table: `%s`\n", err, "salespayments")
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		grpcLog.Errorf("MySql SingleInsert: Error %s when finding rows (%d) affected\n", err, rows)

		return err
	}
	if vGeneral.Debuglevel >= 2 {
		grpcLog.Infof("MySql SingleInsert : \t\t\t\t%d rows inserted:\n", rows)

	}
	return nil
}

// Mysql Bulk Inserter
// https://wawand.co/blog/posts/go-multi-inserting-in-batches/
func (pmnts tMSPayments) BulkInsert(msdb *mysql.DB) (err error) {

	var (
		placeholders []string
		vals         []interface{}
	)

	for index, payment := range pmnts {
		placeholders = append(placeholders, "(?, ?, ?, ?, ?)")

		vals = append(vals,
			payment.InvoiceNumber,
			payment.PayDateTime_Ltz,
			payment.PayTimestamp_Epoc,
			payment.Paid,
			payment.FinTransactionID)

		if vGeneral.Debuglevel > 2 {
			grpcLog.Infoln("")
			grpcLog.Infoln(vals[index*5+0])
			grpcLog.Infoln(vals[index*5+1])
			grpcLog.Infoln(vals[index*5+2])
			grpcLog.Infoln(vals[index*5+3])
			grpcLog.Infoln(vals[index*5+4])
		}
	}

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()

	sqlStatement := fmt.Sprintf("INSERT INTO salespayments(invoiceNumber, payDateTime_Ltz, payTimestamp_Epoc, paid, finTransactionId) VALUES %s", strings.Join(placeholders, ","))

	if vGeneral.Debuglevel > 2 {
		grpcLog.Infof("MySql BulkInsert: %s\n", sqlStatement)
	}

	stmt, err := msdb.Prepare(sqlStatement)
	if err != nil {
		grpcLog.Errorf("MySql BulkInsert: Error %s when preparing SQL statement `%s`\n", err, sqlStatement)
		return err
	}

	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, vals...)
	if err != nil {
		grpcLog.Errorf("MySql BulkInsert: Error %s when inserting row into table: `%s`\n", err, "salespayments")
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		grpcLog.Errorf("MySql BulkInsert: Error %s when finding rows (%d) affected\n", err, rows)
		return err
	}

	return nil

}

// https://www.calhoun.io/connecting-to-a-postgresql-database-with-gos-database-sql-package/
// PostgreSql URL Builder
func (vPgsqldb tPgsqldb) URL() string {

	return fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable",
		vPgsqldb.Hostname,
		vPgsqldb.Port,
		vPgsqldb.Username,
		vPgsqldb.Password,
		vPgsqldb.Dbname,
	)
}

// PostgreSql Connection Creator
func (vPgsqldb tPgsqldb) Connection() (pgdb *pgsql.DB, err error) {

	pgdb, err = pgsql.Open("postgres", vPgsqldb.URL())
	if err != nil {
		grpcLog.Errorf("* connectPostgresDB: Error %s when opening PostgreSqlDB\n", err)
		return nil, err
	}
	grpcLog.Infoln("* connectPostgresDB: Succesffully Opened PostgreSqlDB Connection")

	err = pgdb.Ping()
	if err != nil {
		grpcLog.Errorf("* connectPostgresDB: Errors %s pinging PostgreSqlDB\n", err)
		return nil, err
	}
	grpcLog.Infoln("* connectPostgresDB: Successfully Pinged PostgreSqlDB Client")

	grpcLog.Infof("* connectPostgresDB: Successfully Connected to PostgreSqlDB: (%s)\n", vPgsqldb.Dbname)

	return pgdb, err
}

// PostgreSql Single Inserter
func (pmnt tPGPayment) SingleInsert(pgdb *pgsql.DB) (err error) {

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()

	sqlStatement := "INSERT INTO public.salespayments (invoiceNumber, payDateTime_Ltz, payTimestamp_Epoc, paid, finTransactionId) VALUES ($1, $2, $3, $4, $5)"
	stmt, err := pgdb.PrepareContext(ctx, sqlStatement)
	if err != nil {
		grpcLog.Errorf("PgSql SingleInsert: Error %s when preparing SQL statement: `%s`\n", err, sqlStatement)
		return err
	}
	defer stmt.Close()

	res, err := stmt.ExecContext(ctx, pmnt.InvoiceNumber, pmnt.PayDateTime_Ltz, pmnt.PayTimestamp_Epoc, pmnt.Paid, pmnt.FinTransactionID)
	if err != nil {
		grpcLog.Errorf("PgSql SingleInsert: Error %s when inserting row into table: `%s`\n", err, "salespayments")
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		grpcLog.Errorf("PgSql SingleInsert: Error %s when finding rows (%d) affected\n", err, rows)
		return err
	}

	if vGeneral.Debuglevel >= 2 {
		grpcLog.Infof("PgSql SingleInsert : \t\t\t\t%d rows inserted:\n", rows)

	}
	return nil

}

// PostgreSql Bulk Inserter
func (pmnts tPGPayments) BulkInsert(pgdb *pgsql.DB) (err error) {

	var (
		placeholders []string
		vals         []interface{}
	)

	for index, payment := range pmnts {
		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)", index*5+1, index*5+2, index*5+3, index*5+4, index*5+5))

		vals = append(vals,
			payment.InvoiceNumber,
			payment.PayDateTime_Ltz,
			payment.PayTimestamp_Epoc,
			payment.Paid,
			payment.FinTransactionID)

		if vGeneral.Debuglevel > 2 {
			grpcLog.Infoln("")
			grpcLog.Infoln(vals[index*5+0])
			grpcLog.Infoln(vals[index*5+1])
			grpcLog.Infoln(vals[index*5+2])
			grpcLog.Infoln(vals[index*5+3])
			grpcLog.Infoln(vals[index*5+4])
		}
	}

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()

	sqlStatement := fmt.Sprintf("INSERT INTO salespayments (invoicenumber, paydatetime_ltz, paytimestamp_epoc, paid, fintransactionid) VALUES %s", strings.Join(placeholders, ","))

	if vGeneral.Debuglevel > 2 {
		grpcLog.Infof("PgSql BulkInsert: %s\n", sqlStatement)
	}

	stmt, err := pgdb.Prepare(sqlStatement)
	if err != nil {
		grpcLog.Errorf("PgSql BulkInsert: Error %s when preparing SQL statement `%s`\n", err, sqlStatement)
		return err
	}

	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, vals...)
	if err != nil {
		grpcLog.Errorf("PgSql BulkInsert: Error %s when inserting row into table: `%s`\n", err, "salespayments")
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		grpcLog.Errorf("PgSql BulkInsert: Error %s when finding rows (%d) affected\n", err, rows)
		return err
	}

	return nil
}

// Pretty Print JSON string
func prettyJSON(ms string) {

	var obj map[string]interface{}

	json.Unmarshal([]byte(ms), &obj)

	// Make a custom formatter with indent set
	f := colorjson.NewFormatter()
	f.Indent = 4

	// Marshall the Colorized JSON
	result, _ := f.Marshal(obj)
	fmt.Println(string(result))

}

// Pretty format JSON String
func xprettyJSON(ms string) string {

	var obj map[string]interface{}

	json.Unmarshal([]byte(ms), &obj)

	// Make a custom formatter with indent set
	f := colorjson.NewFormatter()
	f.Indent = 4

	// Marshall the Colorized JSON
	result, _ := f.Marshal(obj)
	//fmt.Println(string(result))
	return string(result)
}

// Transform JSON to BSON for Mongo usage
func JsonToBson(message []byte) ([]byte, error) {
	reader, err := bsonrw.NewExtJSONValueReader(bytes.NewReader(message), true)
	if err != nil {
		return []byte{}, err
	}
	buf := &bytes.Buffer{}
	writer, _ := bsonrw.NewBSONValueWriter(buf)
	err = bsonrw.Copier{}.CopyDocument(writer, reader)
	if err != nil {
		return []byte{}, err
	}
	marshaled := buf.Bytes()
	return marshaled, nil
}

// https://stackoverflow.com/questions/18390266/how-can-we-truncate-float64-type-to-a-particular-precision
func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}
func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func constructFakeBasket() (strct_Basket types.TPBasket, eventTimestamp time.Time, storeName string, err error) {

	// Fake Data etc, not used much here though
	// https://github.com/brianvoe/gofakeit
	// https://pkg.go.dev/github.com/brianvoe/gofakeit

	gofakeit.Seed(0)

	var strct_store types.TPStoreStruct
	var strct_clerk types.TPClerkStruct
	var strct_BasketItem types.TPBasketItems
	var strct_BasketItems []types.TPBasketItems

	if vGeneral.Store == 0 {
		// Determine how many Stores we have in seed file,
		// and build the 2 structures from that viewpoint
		storeCount := len(vSeed.Stores) - 1
		nStoreId := gofakeit.Number(0, storeCount)
		strct_store.Id = vSeed.Stores[nStoreId].Id
		strct_store.Name = vSeed.Stores[nStoreId].Name
		// Want to add lat/long for store
		// Want to add contact details for store

	} else {
		// We specified a specific store
		strct_store.Id = vSeed.Stores[vGeneral.Store].Id
		strct_store.Name = vSeed.Stores[vGeneral.Store].Name
		// Want to add lat/long for store
		// Want to add contact details for store

	}

	// Determine how many Clerks we have in seed file,
	// We want to change this to a function call, where we select a clerk that work at the previous selected store
	clerkCount := len(vSeed.Clerks) - 1
	nClerkId := gofakeit.Number(0, clerkCount)

	// We assign based on fields as we don't want to injest the entire clerk structure, (it includes in the seed file the storeId
	// which is there for us to be able to filter based on store in a later version).
	strct_clerk.Id = vSeed.Clerks[nClerkId].Id
	strct_clerk.Name = vSeed.Clerks[nClerkId].Name
	strct_clerk.Surname = vSeed.Clerks[nClerkId].Surname

	// Uniqiue reference to the basket/sale
	txnId := uuid.New().String()

	// time that everything happened, the 1st as a Unix Epoc time representation,
	// the 2nd in nice human readable milli second representation.
	eventTimestamp = time.Now()
	eventTime_ltz := eventTimestamp.Format("2006-01-02T15:04:05.000") + vGeneral.TimeOffset // Adding the offset is whats causing this value to be ltz

	// How many potential products do we have
	productCount := len(vSeed.Products) - 1
	// now pick from array a random products to add to basket, by using 1 as a start point we ensure we always have at least 1 item.
	nBasketItems := gofakeit.Number(1, vGeneral.Max_items_basket)

	total_amount := 0.0

	for count := 0; count < nBasketItems; count++ {

		productId := gofakeit.Number(0, productCount)

		quantity := gofakeit.Number(1, vGeneral.Max_quantity)
		price := vSeed.Products[productId].Price

		strct_BasketItem = types.TPBasketItems{
			Id:       vSeed.Products[productId].Id,
			Name:     vSeed.Products[productId].Name,
			Brand:    vSeed.Products[productId].Brand,
			Category: vSeed.Products[productId].Category,
			Price:    price,
			Quantity: quantity,
			Subtotal: toFixed(price*float64(quantity), 2),
		}
		strct_BasketItems = append(strct_BasketItems, strct_BasketItem)

		total_amount = total_amount + price*float64(quantity) // Prices shown/listed is inclusive of vat.

	}

	vat_amount := total_amount / (1 + vGeneral.Vatrate) // sales tax
	nett_amount := total_amount - vat_amount            // Total - vat

	terminalPoint := gofakeit.Number(1, vGeneral.Terminals)

	strct_Basket = types.TPBasket{
		InvoiceNumber:      txnId,
		SaleDateTime_Ltz:   eventTime_ltz,
		SaleTimestamp_Epoc: fmt.Sprint(eventTimestamp.UnixMilli()),
		TerminalPoint:      strconv.Itoa(terminalPoint),
		Nett:               toFixed(nett_amount, 2),
		Vat:                toFixed(vat_amount, 2),
		Total:              toFixed(total_amount, 2),
		Store:              strct_store,
		Clerk:              strct_clerk,
		BasketItems:        strct_BasketItems,
	}

	return strct_Basket, eventTimestamp, strct_store.Name, nil
}

func constructPayments(txnId string, salesTimestamp time.Time, total_amount float64) (strct_Payment types.TPPayment, err error) {

	// We're saying payment can be now (salesTimestamp) and up to 5min and 59 seconds later
	payTimestamp := salesTimestamp.Local().Add(time.Minute*time.Duration(gofakeit.Number(0, 5)) + time.Second*time.Duration(gofakeit.Number(0, 59)))
	payTime_ltz := payTimestamp.Format("2006-01-02T15:04:05.000") + vGeneral.TimeOffset // Adding the offset is whats causing this value to be ltz

	strct_Payment = types.TPPayment{
		InvoiceNumber:     txnId,
		PayDateTime_Ltz:   payTime_ltz,
		PayTimestamp_Epoc: fmt.Sprint(payTimestamp.UnixMilli()),
		Paid:              toFixed(total_amount, 2),
		FinTransactionID:  uuid.New().String(),
	}

	return strct_Payment, nil
}

// Big worker... This is where all the magic is called from, ha ha.
func runLoader(arg string) {

	var err error

	var kp *kafka.Producer
	var client schemaregistry.Client
	var serializer *avrov2.Serializer

	var mongoDB *mongo.Database
	var salesbasketcol *mongo.Collection
	var salespaymentcol *mongo.Collection
	var ms *mysql.DB
	var pg *pgsql.DB
	var f_salesbasket *os.File
	var f_salespmnt *os.File

	var vMSPayment tMSPayment
	var vMSPayments tMSPayments
	var vPGPayment tPGPayment
	var vPGPayments tPGPayments

	// Initialize the vGeneral struct variable - This holds our configuration settings.
	vGeneral = vGeneral.load(arg)
	if vGeneral.EchoConfig == 1 {
		vGeneral.Config()
	}

	// Lets get Seed Data from the specified seed file
	vSeed = vSeed.load(vGeneral.SeedConfigFile)

	// Initiale the vKafka struct variable - This holds our Confluent Kafka configuration settings.
	// if Kafka is enabled then create the confluent kafka connection session/objects
	if vGeneral.KafkaEnabled == 1 {

		vKafka = vKafka.load(vGeneral, arg)
		if vGeneral.EchoConfig == 1 {
			vKafka.Config()
		}

		// --
		// Create Producer instance
		// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewProducer

		cm := kafka.ConfigMap{
			"bootstrap.servers":       vKafka.Bootstrapservers,
			"broker.version.fallback": "0.10.0.0",
			"api.version.fallback.ms": 0,
			"client.id":               vGeneral.Hostname,
		}

		if vGeneral.Debuglevel > 0 {
			grpcLog.Info("* runLoader: Basic Client ConfigMap compiled")

		}

		if vKafka.Sasl_mechanisms != "" {
			cm["sasl.mechanisms"] = vKafka.Sasl_mechanisms
			cm["security.protocol"] = vKafka.Security_protocol
			cm["sasl.username"] = vKafka.Sasl_username
			cm["sasl.password"] = vKafka.Sasl_password
			if vGeneral.Debuglevel > 0 {
				grpcLog.Info("* runLoader: Security Authentifaction configured in ConfigMap")

			}
			fmt.Println("Mechanism", vKafka.Sasl_mechanisms)
			fmt.Println("Username", vKafka.Sasl_username)
			fmt.Println("Password", vKafka.Sasl_password)
			fmt.Println("Broker", vKafka.Bootstrapservers)
		}

		// Variable p holds the new Producer instance.
		kp, err = kafka.NewProducer(&cm)
		if err != nil {
			grpcLog.Fatalf("* runLoader: Failed to create Kafka producer: %s\n", err)

		}
		defer kp.Close()

		// Check for errors in creating the Producer
		if err != nil {
			grpcLog.Errorf("* runLoader: 😢Oh noes, there's an error creating the Producer! %s\n", err)

			if ke, ok := err.(kafka.Error); ok {
				switch ec := ke.Code(); ec {
				case kafka.ErrInvalidArg:
					grpcLog.Errorf("* runLoader: 😢 Can't create the producer because you've configured it wrong (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, err)

				default:
					grpcLog.Errorf("* 😢 Can't create the producer (Kafka error code %d)\n\tError: %v\n", ec, err)
				}

			} else {
				// It's not a kafka.Error
				grpcLog.Errorf("* runLoader: 😢 Oh noes, there's a generic error creating the Producer! %v\n", err.Error())
			}
			// call it when you know it's broken
			os.Exit(1)

		}

		// Create a new Schema Registry client
		client, err = schemaregistry.NewClient(schemaregistry.NewConfig(vKafka.SchemaRegistryURL))
		if err != nil {
			grpcLog.Fatalf("* runLoader: Failed to create Schema Registry client: %s\n", err)

		}

		serdeConfig := avrov2.NewSerializerConfig()
		serdeConfig.AutoRegisterSchemas = false
		serdeConfig.UseLatestVersion = true
		//serdeConfig.EnableValidation = true

		//serializer, err = avro.NewGenericSerializer(client, serde.ValueSerde, serdeConfig)
		serializer, err = avrov2.NewSerializer(client, serde.ValueSerde, serdeConfig)
		if err != nil {
			grpcLog.Fatalf("* runLoader: Failed to create Avro serializer: %s\n", err)

		}

		if vGeneral.Debuglevel > 0 {
			grpcLog.Infoln("* runLoader: Sucessfully created Kafka Producer instance")
			grpcLog.Infoln("")
		}
	}

	// Initiale the vMongodb struct variable - This holds our Mongo configuration settings.
	if vGeneral.MongoAtlasEnabled == 1 {

		vMongodb = vMongodb.load(vGeneral, arg)
		if vGeneral.EchoConfig == 1 {
			vMongodb.Config()
		}

		mongoDB, err = vMongodb.Connection(vMongodb)
		if err != nil {
			grpcLog.Fatalf("* runLoader: MongoDB Database Connection Failed: %s\n", err)
		}

		// Define the MongoDB Collection Objects
		salesbasketcol = mongoDB.Collection(vMongodb.Basketcollection)
		salespaymentcol = mongoDB.Collection(vMongodb.Paymentcollection)

	}

	// Initiale the vMysqldb struct variable - This holds our MySqlDB configuration settings.
	if vGeneral.MysqlEnabled == 1 {

		vMysqldb = vMysqldb.load(vGeneral, arg)
		if vGeneral.EchoConfig == 1 {
			vMysqldb.Config()

		}

		ms, err = vMysqldb.Connection()
		if err != nil {
			grpcLog.Fatalf("* runLoader: MySqlDB Database Connection Failed: %s\n", err)

		}
		defer ms.Close()

	}

	// Initiale the vPgsqldb struct variable - This holds our PostgreSqlDB configuration settings.
	if vGeneral.PgsqlEnabled == 1 {

		vPgsqldb = vPgsqldb.load(vGeneral, arg)
		if vGeneral.EchoConfig == 1 {
			vPgsqldb.Config()

		}

		pg, err = vPgsqldb.Connection()
		if err != nil {
			grpcLog.Fatalf("* runLoader: PostgreSqlDB Database Connection Failed: %s\n", err)

		}
		defer pg.Close()

	}

	//We've said we want to safe records to file so lets initiale the file handles etc.
	if vGeneral.Json_to_file == 1 {

		// each time we run, and say we want to store the data created to disk, we create a pair of files for that run.
		// this runId is used as the file name, prepended to either _basket.json or _pmnt.json
		runId = uuid.New().String()

		// Open file -> Baskets
		loc_basket := fmt.Sprintf("%s%s%s_%s.json", vGeneral.Output_path, pathSep, runId, "basket")
		if vGeneral.Debuglevel > 2 {
			grpcLog.Infoln("SalesBasket File     :", loc_basket)

		}

		f_salesbasket, err = os.OpenFile(loc_basket, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			grpcLog.Fatalf("os.OpenFile error A", err)

		}
		defer f_salesbasket.Close()

		// Open file -> Payment
		loc_pmnt := fmt.Sprintf("%s%s%s_%s.json", vGeneral.Output_path, pathSep, runId, "pmnt")
		if vGeneral.Debuglevel > 2 {
			grpcLog.Infoln("SalesPayment File    :", loc_basket)

		}

		f_salespmnt, err = os.OpenFile(loc_pmnt, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			grpcLog.Fatalf("os.OpenFile error A", err)

		}
		defer f_salespmnt.Close()
	}

	if vGeneral.Debuglevel > 0 {
		grpcLog.Infoln("*")
		grpcLog.Info("************** LETS GO Processing ****************************")
		grpcLog.Infoln("")

	}

	// if set to 0 then we want it to simply just run and run and run. so lets give it a pretty big number
	if vGeneral.Testsize == 0 {
		vGeneral.Testsize = 10000000000000
	}

	//
	// For signalling termination from main to go-routine
	var termChan = make(chan bool, 1)
	// For signalling that termination is done from go-routine to main
	var doneChan = make(chan bool)

	var salesbasketdocs = make([]interface{}, vMongodb.Batch_size)
	var salespaymentdocs = make([]interface{}, vMongodb.Batch_size)

	var json_SalesBasket []byte
	var json_SalesPayment []byte

	// We will use this to remember when we last flushed the kafka queues, mongo collection MySql and Postgresql Tables
	var vFlush = 0
	var msg_mongo_count = 0
	var msg_mysql_count = 0
	var msg_pgsql_count = 0

	// this is to keep record of the total batch run time
	var vStart = time.Now()
	for count := 0; count < vGeneral.Testsize; count++ {

		reccount := fmt.Sprintf("%v", count+1)

		// We're going to time every record and push that to prometheus -- EVENTUALLY
		txnStart := time.Now()

		// Build an sales basket
		strct_SalesBasket, eventTimestamp, storeName, err := constructFakeBasket()
		if err != nil {
			grpcLog.Fatalf("Fatal construct Fake SalesBasket: %s", err)

		}

		// Lets sleep a bit before creating SalesPayment, human behaviour... looking for wallet, card ;)
		if vGeneral.Sleep > 0 {
			n := rand.Intn(vGeneral.Sleep)
			time.Sleep(time.Duration(n) * time.Millisecond)
		}

		// Build the sales payment record for the previous created sales basket
		strct_SalesPayment, err := constructPayments(strct_SalesBasket.InvoiceNumber, eventTimestamp, strct_SalesBasket.Total)
		if err != nil {
			grpcLog.Fatalf("Fatal construct Fake SalesPayments: %s", err)

		}

		//if vGeneral.Debuglevel >= 2 || vGeneral.MongoAtlasEnabled == 1 || vGeneral.Json_to_file == 1 {

		json_SalesBasket, err = json.Marshal(strct_SalesBasket)
		if err != nil {
			grpcLog.Fatalf("JSON Marshal to json_SalesBasket Failed: %s", err)

		}

		json_SalesPayment, err = json.Marshal(strct_SalesPayment)
		if err != nil {
			grpcLog.Fatalf("JSON Marshal to json_SalesPayment Failed: %s", err)

		}

		// echo to screen
		if vGeneral.Debuglevel > 2 {
			prettyJSON(string(json_SalesBasket))
			prettyJSON(string(json_SalesPayment))
		}

		//}

		// Post to Confluent Kafka - if enabled
		if vGeneral.KafkaEnabled == 1 {

			if vGeneral.Debuglevel >= 2 {
				grpcLog.Info("")
				grpcLog.Infoln("Posting to Confluent Kafka topics is :\tenabled")
			}

			var payload []byte
			var kafkaMsg kafka.Message

			// -------------------------------- Sales Basket ---------------------------------------------------

			var intf_SalesBasket interface{} = &strct_SalesBasket

			// We need to serialize the Sales Basket interface into a byte array so we can post it to Kafka
			payload, err = serializer.Serialize(vKafka.BasketTopicname, intf_SalesBasket)
			if err != nil {
				grpcLog.Fatalf("Oops, we had a problem Serializeing SalesBasket payload, %s\n", err)

			}

			kafkaMsg = kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &vKafka.BasketTopicname,
					Partition: kafka.PartitionAny,
				},
				Value: payload,           // This is the payload/body thats being posted
				Key:   []byte(storeName), // We us this to group the same transactions together in order, IE submitting/Debtor Bank.
			}

			// This is where we publish message onto the topic... on the Confluent cluster for now,
			if err := kp.Produce(&kafkaMsg, nil); err != nil {
				grpcLog.Fatalf("😢 Darn, there's an error producing the SalesBasket message! %s\n", err.Error())

			}

			// -------------------------------- Sales Basket Done ---------------------------------------------------

			// -------------------------------- Sales Payment ---------------------------------------------------

			if vGeneral.KafkaEnabled == 1 && vGeneral.MysqlEnabled == 0 && vGeneral.PgsqlEnabled == 0 {
				var intf_SalesPayment interface{} = &strct_SalesPayment

				// We need to serialize the Sales Basket interface into a byte array so we can post it to Kafka
				payload, err = serializer.Serialize(vKafka.PaymentTopicname, intf_SalesPayment)
				if err != nil {
					grpcLog.Errorf("Oops, we had a problem Serializeing SalesPayment payload, %s\n", err)

				}

				kafkaMsg = kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &vKafka.PaymentTopicname,
						Partition: kafka.PartitionAny,
					},
					Value: payload,           // This is the payload/body thats being posted
					Key:   []byte(storeName), // We us this to group the same transactions together in order, IE submitting/Debtor Bank.
				}

				// This is where we publish message onto the topic... on the Confluent cluster for now,
				if err := kp.Produce(&kafkaMsg, nil); err != nil {
					grpcLog.Fatalf("😢 Darn, there's an error producing the SalesPayment message! %s", err.Error())

				}

			}

			// -------------------------------- Sales Payment Done ---------------------------------------------------

			vFlush++

			// Fush every flush_interval loops
			if vFlush == vKafka.Flush_interval {
				t := vKafka.Flush_timeout // Timeout of 1 second, 10000 milliseconds
				n := time.Now()
				if r := kp.Flush(t); r > 0 {
					grpcLog.Errorf("Failed to flush all messages after %d milliseconds. %d message(s) remain\n", t, r)

				} else {
					if vGeneral.Debuglevel >= 2 {
						grpcLog.Infof("Total Kafka Messages : \t\t\t%v, Flushing every %v Messages took %s,\n", count, vFlush, time.Since(n))

					}
					vFlush = 0
				}
			}

			// We will decide if we want to keep this bit!!! or simplify it.
			//
			// Convenient way to Handle any events (back chatter) that we get
			go func() {
				doTerm := false
				for !doTerm {
					// The `select` blocks until one of the `case` conditions
					// are met - therefore we run it in a Go Routine.
					select {
					case ev := <-kp.Events():
						// Look at the type of Event we've received
						switch ev.(type) {

						case *kafka.Message:
							// It's a delivery report
							km := ev.(*kafka.Message)
							if km.TopicPartition.Error != nil {
								grpcLog.Errorf("☠️ Failed to send message to topic '%v'\tErr: %v\n",
									string(*km.TopicPartition.Topic),
									km.TopicPartition.Error)

							} else {
								if vGeneral.Debuglevel > 2 {
									grpcLog.Infof("Message delivered to topic : \t\t\t%v (partition %d at offset %d)\n",
										string(*km.TopicPartition.Topic),
										km.TopicPartition.Partition,
										km.TopicPartition.Offset)

								}
							}

						case kafka.Error:
							// It's an error
							em := ev.(kafka.Error)
							grpcLog.Errorf("☠️ Uh oh, caught an error:\t%v\n", em)

						}
					case <-termChan:
						doTerm = true

					}
				}
				close(doneChan)
			}()

		}

		// Do we want to insertrecords/documents directly into Mongo Atlas?
		if vGeneral.MongoAtlasEnabled == 1 {

			if vGeneral.Debuglevel >= 2 {
				grpcLog.Infoln("Posting to MongoDB collection is: \t\tenabled")
			}

			msg_mongo_count += 1

			// Flush/insert
			// Cast a byte string to BSon
			// https://stackoverflow.com/questions/39785289/how-to-marshal-json-string-to-bson-document-for-writing-to-mongodb
			// this way we don't need to care what the source structure is, it is all cast and inserted into the defined collection.

			salesbasketdoc, err := JsonToBson(json_SalesBasket)
			if err != nil {
				grpcLog.Errorf("Oops, we had a problem JsonToBson converting the SalesBasket payload: %s\n", err)

			}

			salespaymentdoc, err := JsonToBson(json_SalesPayment)
			if err != nil {
				grpcLog.Errorf("Oops, we had a problem JsonToBson converting the SalesPayment payload, %s\n", err)

			}

			// Single Record inserts
			if vMongodb.Batch_size == 1 {

				// Sales Basket

				// Time to get this into the MondoDB Collection
				result, err := salesbasketcol.InsertOne(context.TODO(), salesbasketdoc)
				if err != nil {
					grpcLog.Errorf("Oops, we had a problem inserting the SalesBasket document: %s\n", err)

				}

				if vGeneral.Debuglevel > 2 {
					// When you run this file, it should print:
					// Document inserted with ID: ObjectID("...")
					grpcLog.Infof("Mongo SalesBasket Doc inserted with ID: %s\n", result.InsertedID)

				}
				if vGeneral.Debuglevel > 2 {
					// prettyJSON takes a string which is actually JSON and makes it's pretty, and prints it.
					prettyJSON(string(json_SalesBasket))

				}

				// Payment

				// Time to get this into the MondoDB Collection
				result, err = salespaymentcol.InsertOne(context.TODO(), salespaymentdoc)
				if err != nil {
					grpcLog.Errorf("Oops, we had a problem inserting the SalesPayment document: %s\n", err)

				}

				if vGeneral.Debuglevel > 2 {
					// When you run this file, it should print:
					// Document inserted with ID: ObjectID("...")
					grpcLog.Infoln("Mongo SalesPayment Doc inserted with ID: ", result.InsertedID, "\n")

				}
				if vGeneral.Debuglevel > 2 {
					// prettyJSON takes a string which is actually JSON and makes it's pretty, and prints it.
					prettyJSON(string(json_SalesPayment))

				}

			} else {

				// Sales Basket
				salesbasketdocs[msg_mongo_count-1] = salesbasketdoc
				salespaymentdocs[msg_mongo_count-1] = salespaymentdoc

				if msg_mongo_count%vMongodb.Batch_size == 0 {

					// Time to get this into the MondoDB Collection

					// Sales Basket
					n := time.Now()

					_, err = salesbasketcol.InsertMany(context.TODO(), salesbasketdocs)
					if err != nil {
						grpcLog.Errorf("Oops, we had a problem inserting (IM) the document, %s\n", err)

					}
					if vGeneral.Debuglevel >= 2 {
						grpcLog.Infof("Mongo Bulk Insert SalesBaskets :\t\t%d took %s \n", msg_mongo_count, time.Since(n))

					}

					// Payment
					n = time.Now()

					_, err = salespaymentcol.InsertMany(context.TODO(), salespaymentdocs)
					if err != nil {
						grpcLog.Errorf("Oops, we had a problem inserting (IM) the document, %s\n", err)

					}
					if vGeneral.Debuglevel >= 2 {
						grpcLog.Infof("Mongo Bulk Insert SalesPayment :\t\t%d took %s \n", msg_mongo_count, time.Since(n))

					}

					msg_mongo_count = 0

				}

			}

		} else {
			if vGeneral.Debuglevel >= 2 {
				grpcLog.Infoln("Posting to MongoDB collection is: \t\tdisabled")
			}
		}

		if vGeneral.MysqlEnabled == 1 {

			if vGeneral.Debuglevel >= 2 {
				grpcLog.Infoln("Posting to MySqlDB table is: \t\t\tenabled")
			}

			msg_mysql_count += 1

			// Single Record inserts
			if vMysqldb.Batch_size == 1 {

				vMSPayment.InvoiceNumber = strct_SalesPayment.InvoiceNumber
				vMSPayment.PayDateTime_Ltz = strct_SalesPayment.PayDateTime_Ltz
				vMSPayment.PayTimestamp_Epoc = strct_SalesPayment.PayTimestamp_Epoc
				vMSPayment.Paid = strct_SalesPayment.Paid
				vMSPayment.FinTransactionID = strct_SalesPayment.FinTransactionID

				_ = vMSPayment.SingleInsert(ms)

			} else {
				// Add payment to Payments Array

				var vvMSPayment types.TPPayment
				vvMSPayment.InvoiceNumber = strct_SalesPayment.InvoiceNumber
				vvMSPayment.PayDateTime_Ltz = strct_SalesPayment.PayDateTime_Ltz
				vvMSPayment.PayTimestamp_Epoc = strct_SalesPayment.PayTimestamp_Epoc
				vvMSPayment.Paid = strct_SalesPayment.Paid
				vvMSPayment.FinTransactionID = strct_SalesPayment.FinTransactionID

				vMSPayments = append(vMSPayments, vvMSPayment)

				if msg_mysql_count%vMysqldb.Batch_size == 0 {

					n := time.Now()

					err = vMSPayments.BulkInsert(ms)
					if err == nil && vGeneral.Debuglevel >= 2 {
						grpcLog.Infof("MySql Bulk Inserting SalesPayments :\t\t%d took %s\n", msg_mysql_count, time.Since(n))

					}
					msg_mysql_count = 0
					vMSPayments = nil
				}
			}
		} else {
			if vGeneral.Debuglevel >= 2 {
				grpcLog.Infoln("Posting to MySqlDB table is: \t\t\tdisabled")
			}
		}

		if vGeneral.PgsqlEnabled == 1 {

			if vGeneral.Debuglevel >= 2 {
				grpcLog.Infoln("Posting to PostgreSql table is :\t\tenabled")
			}

			msg_pgsql_count += 1

			// Single Record inserts
			if vPgsqldb.Batch_size == 1 {

				vPGPayment.InvoiceNumber = strct_SalesPayment.InvoiceNumber
				vPGPayment.PayDateTime_Ltz = strct_SalesPayment.PayDateTime_Ltz
				vPGPayment.PayTimestamp_Epoc = strct_SalesPayment.PayTimestamp_Epoc
				vPGPayment.Paid = strct_SalesPayment.Paid
				vPGPayment.FinTransactionID = strct_SalesPayment.FinTransactionID

				_ = vPGPayment.SingleInsert(pg)

			} else {
				// Add payment to Payments Array

				var vvPGPayment types.TPPayment
				vvPGPayment.InvoiceNumber = strct_SalesPayment.InvoiceNumber
				vvPGPayment.PayDateTime_Ltz = strct_SalesPayment.PayDateTime_Ltz
				vvPGPayment.PayTimestamp_Epoc = strct_SalesPayment.PayTimestamp_Epoc
				vvPGPayment.Paid = strct_SalesPayment.Paid
				vvPGPayment.FinTransactionID = strct_SalesPayment.FinTransactionID

				vPGPayments = append(vPGPayments, vvPGPayment)

				if msg_pgsql_count%vPgsqldb.Batch_size == 0 {

					n := time.Now()

					err = vPGPayments.BulkInsert(pg)
					if err == nil && vGeneral.Debuglevel >= 2 {
						grpcLog.Infof("Postgres Bulk Inserting SalesPayments :\t%d took %s\n", msg_pgsql_count, time.Since(n))

					}
					msg_pgsql_count = 0
					vPGPayments = nil
				}
			}
		} else {
			if vGeneral.Debuglevel >= 2 {
				grpcLog.Infoln("Posting to PostgreSql table is :\t\tdisabled")
			}
		}

		// Save multiple Basket docs and Payment docs to a single basket file and single payment file for the run
		if vGeneral.Json_to_file == 1 {

			if vGeneral.Debuglevel >= 2 {
				grpcLog.Infoln("Posting JSON to File is :\t\t\tenabled")
			}

			// Basket
			pretty_basket, err := json.MarshalIndent(strct_SalesBasket, "", " ")
			if err != nil {
				grpcLog.Errorf("Basket MarshalIndent error %s\n", err)

			}

			if _, err = f_salesbasket.WriteString(string(pretty_basket) + ",\n"); err != nil {
				grpcLog.Errorf("Basket os.WriteString error %s\n", err)

			}

			// Payment
			pretty_pmnt, err := json.MarshalIndent(strct_SalesPayment, "", " ")
			if err != nil {
				grpcLog.Errorf("Payment MarshalIndent error %s\n", err)

			}

			if _, err = f_salespmnt.WriteString(string(pretty_pmnt) + ",\n"); err != nil {
				grpcLog.Errorf("Payment os.WriteString error %s\n", err)

			}

		} else {
			if vGeneral.Debuglevel >= 2 {
				grpcLog.Infoln("Posting JSON to File is :\t\t\tdisabled")
			}
		}

		if vGeneral.Debuglevel > 1 {
			grpcLog.Infof("Total Time :\t\t\t\t\t%f %s", time.Since(txnStart).Seconds(), "Sec")

		}
		/*
		   %b  decimalless scientific notation with exponent a power of two,
		       in the manner of strconv.FormatFloat with the 'b' format,
		       e.g. -123456p-78
		   %e  scientific notation, e.g. -1.234456e+78
		   %E  scientific notation, e.g. -1.234456E+78
		   %f  decimal point but no exponent, e.g. 123.456
		   %F  synonym for %f
		   %g  %e for large exponents, %f otherwise. Precision is discussed below.
		   %G  %E for large exponents, %F otherwise
		*/

		// used to slow the data production/posting to kafka and safe to file system down.
		// This mimics the speed with which baskets are presented at terminalpoint.
		// if vGeneral.sleep = 1000, then n will be random value of 0 -> 1000  aka 0 and 1 second
		// the value is seconds based. so to imply every 5 min a new basket is to be presented then it's 5 x 60 x 1000 = 300000
		if vGeneral.Sleep > 0 {
			n := rand.Intn(vGeneral.Sleep)
			if vGeneral.Debuglevel >= 1 {
				if n > 60000 { // 1000 = 1 sec, 6000 = 6 sec, 60000 = 1min
					grpcLog.Infof("Going to sleep for :\t\t\t\t%d Seconds\n", n/1000)

				} else {
					grpcLog.Infof("Going to sleep for :\t\t\t\t%d Milliseconds\n", n)
				}
			}
			time.Sleep(time.Duration(n) * time.Millisecond)
		}

		if vGeneral.Debuglevel >= 1 {
			grpcLog.Infof("Record :\t\t\t\t\t%s Completed", reccount)
			if vGeneral.Debuglevel > 1 {
				grpcLog.Infoln("---------------------------------------------------------------")

			}
		}
	}

	grpcLog.Infoln("")
	grpcLog.Infoln("**** DONE Processing ****")
	grpcLog.Infoln("")

	vEnd := time.Now()
	vElapse := vEnd.Sub(vStart)
	grpcLog.Infoln("Start                         : ", vStart)
	grpcLog.Infoln("End                           : ", vEnd)
	grpcLog.Infoln("Elapsed Time (Seconds)        : ", vElapse.Seconds())
	grpcLog.Infoln("Records Processed             : ", vGeneral.Testsize)
	grpcLog.Infoln(fmt.Sprintf("                              :  %.3f Txns/Second", float64(vGeneral.Testsize)/vElapse.Seconds()))

	grpcLog.Infoln("")

} // runLoader()

func main() {

	var arg string

	grpcLog.Info("***************         Starting           *******************")

	arg = os.Args[1]

	runLoader(arg)

	grpcLog.Info("****************        Completed          ********************")

}
