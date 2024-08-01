package logger

import (
	"log"
	"os"
)

var consoleOutLogger *log.Logger
var consoleErrLogger *log.Logger
var fileOutLogger *log.Logger
var fileErrLogger *log.Logger

var fileOut *os.File
var fileErr *os.File

func CloseLogger() {
	if fileOut != nil {
		err := fileOut.Close()
		if err != nil {
			return
		}
	}
	if fileErr != nil {
		err := fileErr.Close()
		if err != nil {
			return
		}
	}
}

func createLoggers() {
	fileOutPath, fileErrPath := os.Getenv("LOGS_FILE"), os.Getenv("ERROR_LOGS_FILE")

	if fileOutPath != "" {
		f, err := os.OpenFile(fileOutPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Error Opening Out Log File: %v", err)
		} else {
			fileOut = f
		}
	}
	if fileErrPath != "" {
		f, err := os.OpenFile(fileErrPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Error Opening Err Log File: %v", err)
		} else {
			fileErr = f
		}
	}

	prefix := "Metaverses Estate Data Downloader"
	consoleOutLogger = log.New(os.Stdout, prefix+" [Info] ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile|log.LUTC)
	consoleErrLogger = log.New(os.Stdout, prefix+" [Error] ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile|log.LUTC)
	if fileOut != nil {
		fileOutLogger = log.New(fileOut, prefix+" [Info] ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Llongfile|log.LUTC)
	}
	if fileErr != nil {
		fileErrLogger = log.New(fileErr, prefix+" [Error] ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Llongfile|log.LUTC)
	}
}

func InitializeLogger() {
	createLoggers()
}

func Info(message string) {
	if consoleOutLogger != nil {
		consoleOutLogger.Println(message)
	} else {
		log.Println(message)
	}
	if fileOutLogger != nil {
		fileOutLogger.Println(message)
	}
}

func Infof(format string, v ...any) {
	consoleOutLogger.Printf(format, v)
	if fileOutLogger != nil {
		fileOutLogger.Printf(format, v)
	}
}

func Error(message string) {
	if consoleErrLogger != nil {
		consoleErrLogger.Println(message)
	} else {
		log.Println(message)
	}
	if fileErrLogger != nil {
		fileErrLogger.Println(message)
	}
}

func Errore(err error, _panic bool) {
	if consoleErrLogger != nil {
		consoleErrLogger.Println(err)
	} else {
		log.Println(err)
	}
	if fileErrLogger != nil {
		fileErrLogger.Println(err)
	}
	if _panic {
		panic(err)
	}
}

func ErrorE(message string, err error, _panic bool) {
	Error(message)
	if err != nil {
		Errore(err, _panic)
	}
}
