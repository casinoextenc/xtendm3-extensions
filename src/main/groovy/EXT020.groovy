import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * README
 * This extension is used by Mashup
 * <p>
 * Name : EXT020
 * Description : Select the items based on the criteria contained in EXT021 table and add records to the EXT020 table (EXT020MI.SelAssortItems conversion)
 * Date         Changed By   Description
 * 20240118     YYOU         COMX01 - Submit calc EXT022
 */

public class EXT020 extends ExtendM3Batch {

    private final MIAPI mi
    private final DatabaseAPI database
    private final LoggerAPI logger
    private final MICallerAPI miCaller
    private final ProgramAPI program
    private final UtilityAPI utility
    private final BatchAPI batch
    private final TextFilesAPI textFiles
    private int currentCompany
    private String currentDivision
    private String cuno = ""
	  private String svcuno = ""
    private String fvdt = ""
    private String fdat = ""
    private String ascd = ""
    private boolean IN60 = false
    private String rawData
    private int rawDataLength
    private int beginIndex
    private int endIndex
    private String logFileName
    private String jobNumber
    private String currentDate = ""

    public EXT020(LoggerAPI logger, DatabaseAPI database, UtilityAPI utility, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles) {
        this.logger = logger
        this.database = database
        this.program = program
        this.batch = batch
        this.miCaller = miCaller
        this.textFiles = textFiles
        this.utility = utility
    }

    public void main() {
        // Get job number
        LocalDateTime timeOfCreation = LocalDateTime.now()
        jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

        //logger.debug("Début EXT020")
        if (batch.getReferenceId().isPresent()) {
            Optional<String> data = getJobData(batch.getReferenceId().get())
            performActualJob(data)
        } else {
            // No job data found
            //logger.debug("Job data for job ${batch.getJobId()} is missing")
        }
    }

    // Get job data
    private Optional<String> getJobData(String referenceId) {
        def query = database.table("EXTJOB").index("00").selection("EXDATA").build()
        def container = query.createContainer()
        container.set("EXRFID", referenceId)
        if (query.read(container)) {
            //logger.debug("EXDATA = " + container.getString("EXDATA"))
            return Optional.of(container.getString("EXDATA"))
        } else {
            //logger.debug("EXTJOB not found")
        }
        return Optional.empty()
    }

    // Perform actual job
    private performActualJob(Optional<String> data) {
        if (!data.isPresent()) {
            //logger.debug("Job reference Id ${batch.getReferenceId().get()} is passed but data was not found")
            return
        }
        rawData = data.get()
        logger.debug("Début performActualJob")
        String inDATE = getFirstParameter()

        logger.debug("inDATE = " + inDATE)
        
        svcuno = ""
        
        currentCompany = (Integer) program.getLDAZD().CONO
        currentDivision = program.getLDAZD().DIVI
        LocalDateTime timeOfCreation = LocalDateTime.now()

        if (inDATE != null && !inDATE.trim().isBlank()) {
            fdat = inDATE
            if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
                String header = "MSG;" + "FDAT"
                String message = "Date de début est incorrecte " + ";" + fdat
                logMessage(header, message)
                return
            }
        } else {
            fdat = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
        }
        //Lecture des EXT010 modifié
        ExpressionFactory expression_EXT010 = database.getExpressionFactory("EXT010")
        expression_EXT010 = expression_EXT010.ge("EXLMDT", fdat)
        DBAction EXT010_query = database.table("EXT010").index("02").matching(expression_EXT010).selection("EXCUNO").build()
        DBContainer EXT010 = EXT010_query.getContainer()
        EXT010.set("EXCONO", currentCompany)
        if (!EXT010_query.readAll(EXT010, 1, outData_EXT010)) {
        }
    }

    // Retrieve EXT010
    Closure<?> outData_EXT010 = {DBContainer EXT010 ->
        cuno = EXT010.get("EXCUNO")
        logger.debug("EXCUNO traité" + cuno)
        if(cuno != svcuno) {
          DBAction OASCUS_query = database.table("OASCUS").index("00").selection("OCCONO", "OCASCD", "OCCUNO", "OCFDAT").build()
          DBContainer OASCUS = OASCUS_query.getContainer()
          OASCUS.set("OCCONO", currentCompany)
          ascd = EXT010.get("EXCUNO") as String 
          logger.debug("OCASCD à selectionner" + ascd.trim() +"0")
          OASCUS.set("OCASCD", ascd.trim() + "0")
          OASCUS.set("OCCUNO", cuno)
          if (!OASCUS_query.readAll(OASCUS, 3, outData_OASCUS)) {
          }
        }
        svcuno = cuno
    }

// Retrieve OASCUS
Closure<?> outData_OASCUS = {DBContainer OASCUS ->
        // Add selected items in the assortment
        fvdt = OASCUS.get("OCFDAT") as String
        ascd = OASCUS.get("OCASCD")
        cuno = OASCUS.get("OCCUNO")
        logger.debug("Lancement executeEXT820MISubmitBatch pour assortiment : " + ascd +"-"+cuno + "-"+fvdt)
        executeEXT820MISubmitBatch(currentCompany as String, "EXT022", ascd, cuno, fvdt, "", "2", "", "", "")
}

// Execute EXT820MI.SubmitBatch
private executeEXT820MISubmitBatch(String CONO, String JOID, String P001, String P002, String P003, String P004, String P005, String P006, String P007, String P008) {
    def parameters = ["CONO":CONO, "JOID":JOID, "P001":P001, "P002":P002, "P003":P003, "P004":P004, "P005":P005, "P006":
    P006, "P007":P007, "P008":P008]
    Closure<?> handler = {Map < String, String > response ->
    if (response.error != null) {
    } else {
    }
    }
    miCaller.call("EXT820MI", "SubmitBatch", parameters, handler)
}

// Get first parameter
private String getFirstParameter() {
    //logger.debug("rawData = " + rawData)
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
    //logger.debug("parameter = " + parameter)
    return parameter
}

// Get next parameter
private String getNextParameter() {
    beginIndex = endIndex + 1
    endIndex = rawDataLength - rawData.indexOf(";") - 1
    rawData = rawData.substring(beginIndex, rawDataLength)
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
    //logger.debug("parameter = " + parameter)
    return parameter
}

// Log message
void logMessage(String header, String message) {
    textFiles.open("FileImport")
    logFileName = "MSG_" + program.getProgramName() + "." + "batch" + "." + jobNumber + ".csv"
    if (!textFiles.exists(logFileName)) {
        log(header)
        log(message)
    }
}

// Log
void log(String message) {
    IN60 = true
    ////logger.debug(message)
    message = LocalDateTime.now().toString() + ";" + message
    Closure<?> consumer = {PrintWriter printWriter ->
            printWriter.println(message)
    }
    textFiles.write(logFileName, "UTF-8", true, consumer)
}
}
