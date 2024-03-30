package database

import (
	"fmt"
	"log"
	"stream/models"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB

var (
	host     = "rain.db.elephantsql.com"
	user     = "tgaryate"
	dbname   = "tgaryate"
	password = "k1HV8_hFYKkX9-ci6FzuHWUTRnAxQAFp"
)

func NewDb() *gorm.DB {

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s  sslmode=disable TimeZone=Asia/Shanghai",
		host, user, password, dbname,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database %v", err.Error())
	}
	log.Printf("Connected to Database")
	DB = db
	DB.AutoMigrate(&models.Message{})
	return DB
}
