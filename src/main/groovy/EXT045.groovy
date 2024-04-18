/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT045
 * Description : allows to plan and submit the schedule job EXT040 for all customers
 * Date         Changed By   Description
 * 20231215     RENARN       COMX02 - Cadencier
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import java.time.LocalDate
import java.time.temporal.WeekFields
import java.util.Locale

public class EXT045 extends ExtendM3Batch {
    private final LoggerAPI logger
    private final DatabaseAPI database
    private final ProgramAPI program
    private final BatchAPI batch
    private final MICallerAPI miCaller
    private final TextFilesAPI textFiles
    private final UtilityAPI utility
    private Integer currentCompany
    private String rawData
    private int rawDataLength
    private int beginIndex
    private int endIndex
    private String logFileName
    private boolean IN60
    private String jobNumber
    private Integer currentDate
    private String newCDNN
    private String customer
    private String assortment
    private String globalOffer
    private String calendar
    private String allContacts
    private String schedule
    private String listAlreadySentCustomers
    private Integer counter

    public EXT045(LoggerAPI logger, DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles, UtilityAPI utility) {
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

        logger.debug("Début" + program.getProgramName())
        //logger.debug("referenceId = " + batch.getReferenceId().get())
        if(batch.getReferenceId().isPresent()){
            Optional<String> data = getJobData(batch.getReferenceId().get())
            logger.debug("data = " + data)
            performActualJob(data)
        } else {
            // No job data found
            logger.debug("Job data for job ${batch.getJobId()} is missing")
        }
    }
    // Get job data
    private Optional<String> getJobData(String referenceId){
        def query = database.table("EXTJOB").index("00").selection("EXDATA").build()
        def container = query.createContainer()
        container.set("EXRFID", referenceId)
        if (query.read(container)){
            logger.debug("EXDATA = " + container.getString("EXDATA"))
            return Optional.of(container.getString("EXDATA"))
        } else {
            logger.debug("EXTJOB not found")
        }
        return Optional.empty()
    }
    // Perform actual job
    private performActualJob(Optional<String> data){
        if(!data.isPresent()){
            logger.debug("Job reference Id ${batch.getReferenceId().get()} is passed but data was not found")
            return
        }
        rawData = data.get()
        logger.debug("Début performActualJob")

        currentCompany = (Integer)program.getLDAZD().CONO

        LocalDateTime timeOfCreation = LocalDateTime.now()
        currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer

        // Perform Job
        listAlreadySentCustomers = ""
        counter = 0
        ExpressionFactory expression = database.getExpressionFactory("OASCUS")
        expression = expression.le("OCFDAT", currentDate as String).and(expression.ge("OCTDAT", currentDate as String))
        DBAction queryOASCUS = database.table("OASCUS").index("00").matching(expression).selection("OCCUNO","OCASCD").build()
        DBContainer OASCUS = queryOASCUS.getContainer()
        OASCUS.set("OCCONO", currentCompany)
        if (!queryOASCUS.readAll(OASCUS, 1, outData_OASCUS)) {
        }

        // Delete file EXTJOB
        //deleteEXTJOB()
    }
    // Retrieve OASCUS
    Closure<?> outData_OASCUS = { DBContainer OASCUS ->
        customer = OASCUS.get("OCCUNO")
        assortment = OASCUS.get("OCASCD")

        //if(assortment.trim() == (customer.trim()+"0") && !listAlreadySentCustomers.contains(customer) && counter < 1000) {
        if(assortment.trim() == (customer.trim()+"0") && !listAlreadySentCustomers.contains(customer) && counter < 5) {
            counter++
            if(listAlreadySentCustomers == ""){
                listAlreadySentCustomers = "("+customer+")"
            } else {
                listAlreadySentCustomers = listAlreadySentCustomers + "|" + "("+customer+")"
            }
            logger.debug("listAlreadySentCustomers = " + listAlreadySentCustomers)

            newCDNN = ""
            executeEXT040MIRtvNextCalendar(currentCompany as String, customer)
            calendar = newCDNN

            globalOffer = "0"

            LocalDateTime timeOfCreation = LocalDateTime.now()
            DBAction query = database.table("EXT042").index("00").build()
            DBContainer EXT042 = query.getContainer()
            EXT042.set("EXCONO", currentCompany)
            EXT042.set("EXCUNO", customer)
            EXT042.set("EXCDNN", calendar)
            EXT042.set("EXASCD", assortment)
            if (!query.read(EXT042)) {
                EXT042.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
                EXT042.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
                EXT042.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
                EXT042.setInt("EXCHNO", 1)
                EXT042.set("EXCHID", program.getUser())
                query.insert(EXT042)
            }

            allContacts = "1"
            schedule = "1"

            logger.debug("assortment = " + assortment)
            logger.debug("executeEXT820MISubmitBatch :")
            logger.debug("customer = " + customer)
            logger.debug("calendar = " + calendar)
            logger.debug("globalOffer = " + globalOffer)
            logger.debug("allContacts = " + allContacts)
            logger.debug("schedule = " + schedule)

            //if(formatTXT.trim() != "" || formatCSV.trim() != "" || formatXLSX.trim() != ""){
              //executeEXT820MISubmitBatch(currentCompany as String, "EXT040", customer, calendar, globalOffer, formatCSV, formatTXT, formatXLSX, allContacts, schedule, "")
              executeEXT820MISubmitBatch(currentCompany as String, "EXT040", customer, calendar, allContacts, schedule, "", "", "", "", "")
            //}
        }
    }
    // Execute EXT040MI.RtvNextCalendar
    private executeEXT040MIRtvNextCalendar(String CONO, String CUNO){
        def parameters = ["CONO": CONO, "CUNO": CUNO]
        Closure<?> handler = { Map<String, String> response ->
            if (response.error != null) {
            } else {
                newCDNN = response.CDNN.trim()
            }
        }
        miCaller.call("EXT040MI", "RtvNextCalendar", parameters, handler)
    }
    // Execute EXT820MI.SubmitBatch
    //private executeEXT820MISubmitBatch(String CONO, String JOID, String P001, String P002, String P003, String P004, String P005, String P006, String P007, String P008, String P009){
    private executeEXT820MISubmitBatch(String CONO, String JOID, String P001, String P002, String P003, String P004, String P005, String P006, String P007, String P008, String P009){
        def parameters = ["CONO": CONO, "JOID": JOID, "P001": P001, "P002": P002, "P003": P003, "P004": P004, "P005": P005, "P006": P006, "P007": P007, "P008": P008, "P009": P009]
        Closure<?> handler = { Map<String, String> response ->
            if (response.error != null) {
            } else {
            }
        }
        miCaller.call("EXT820MI", "SubmitBatch", parameters, handler)
    }
    // Get first parameter
    private String getFirstParameter(){
        logger.debug("rawData = " + rawData)
        rawDataLength = rawData.length()
        beginIndex = 0
        endIndex = rawData.indexOf(";")
        // Get parameter
        String parameter = rawData.substring(beginIndex, endIndex)
        logger.debug("parameter = " + parameter)
        return parameter
    }
    // Get next parameter
    private String getNextParameter(){
        beginIndex = endIndex + 1
        endIndex = rawDataLength - rawData.indexOf(";") - 1
        rawData = rawData.substring(beginIndex, rawDataLength)
        rawDataLength = rawData.length()
        beginIndex = 0
        endIndex = rawData.indexOf(";")
        // Get parameter
        String parameter = rawData.substring(beginIndex, endIndex)
        logger.debug("parameter = " + parameter)
        return parameter
    }
    // Delete records related to the current job from EXTJOB table
    public void deleteEXTJOB(){
        LocalDateTime timeOfCreation = LocalDateTime.now()
        DBAction query = database.table("EXTJOB").index("00").build()
        DBContainer EXTJOB = query.getContainer()
        EXTJOB.set("EXRFID", batch.getReferenceId().get())
        if(!query.readAllLock(EXTJOB, 1, updateCallBack_EXTJOB)){
        }
    }
    // Delete EXTJOB
    Closure<?> updateCallBack_EXTJOB = { LockedResult lockedResult ->
        lockedResult.delete()
    }
    // Log message
    void logMessage(String header, String message) {
        textFiles.open("FileImport")
        logFileName = "MSG_" + program.getProgramName() + "." + "batch" + "." + jobNumber + ".csv"
        if(!textFiles.exists(logFileName)) {
            log(header)
            log(message)
        }
    }
    // Log
    void log(String message) {
        IN60 = true
        //logger.debug(message)
        message = LocalDateTime.now().toString() + ";" + message
        Closure<?> consumer = { PrintWriter printWriter ->
            printWriter.println(message)
        }
        textFiles.write(logFileName, "UTF-8", true, consumer)
    }
}
