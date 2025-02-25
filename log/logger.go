package mylog // Consider renaming the package to avoid conflict with the standard library  

import (  
    "os"  
    "github.com/cihub/seelog"  
)  

// InitLogger initializes the logger with the configuration file given.  
// It returns an error if the configuration file cannot be loaded.  
func InitLogger(cfgfile string) error {  
    // Check if the configuration file exists  
    if _, err := os.Stat(cfgfile); os.IsNotExist(err) {  
        return err // Return an error if the file does not exist  
    }  

    logger, err := seelog.LoggerFromConfigAsFile(cfgfile)  
    if err != nil {  
        return err // Return the error instead of panicking  
    }  

    seelog.ReplaceLogger(logger)  
    return nil // Return nil if initialization is successful  
}
