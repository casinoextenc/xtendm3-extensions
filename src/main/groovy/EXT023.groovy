/**
 * README
 * This extension is used by batch EXT022
 *
 * Name : EXT023
 * Description : Read EXT022 table and call "CRS105MI/AddAssmItem" for each item (EXT023MI.AddAssortItems conversion)
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01- Add assortment
 * 20240206		YVOYOU		 COMX01 - Exclude item management
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class EXT023 extends ExtendM3Batch {
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
	private String ascd = ""
	private String cuno = ""
	private String fdat = ""
	private String itno = ""
	private boolean exclu

	public EXT023(LoggerAPI logger, DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles, UtilityAPI utility) {
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

		logger.info("Début" + program.getProgramName())
		//logger.info("referenceId = " + batch.getReferenceId().get())
		if(batch.getReferenceId().isPresent()){
			Optional<String> data = getJobData(batch.getReferenceId().get())
			//logger.info("data = " + data)
			performActualJob(data)
		} else {
			// No job data found
			logger.error("Job data for job ${batch.getJobId()} is missing")
		}
	}
	private Optional<String> getJobData(String referenceId){
		def query = database.table("EXTJOB").index("00").selection("EXDATA").build()
		def container = query.createContainer()
		container.set("EXRFID", referenceId)
		if (query.read(container)){
			logger.info("EXDATA = " + container.getString("EXDATA"))
			return Optional.of(container.getString("EXDATA"))
		} else {
			logger.error("EXTJOB not found")
		}
		return Optional.empty()
	}
	private performActualJob(Optional<String> data){
		if(!data.isPresent()){
			logger.error("Job reference Id ${batch.getReferenceId().get()} is passed but data was not found")
			return
		}
		rawData = data.get()
		logger.info("Début performActualJob")
		String inASCD = getFirstParameter()
		String inCUNO = getNextParameter()
		String inFDAT = getNextParameter()

		currentCompany = (Integer)program.getLDAZD().CONO

		// Perform Job
		ascd = inASCD
		cuno = inCUNO

		fdat =""
		if (inFDAT == null){
			String header = "MSG"
			String message = "Date de début est obligatoire"
			logMessage(header, message)
			return
		} else {
			fdat = inFDAT
			if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
				String header = "MSG"
				String message = "Date de début est invalide"
				logMessage(header, message)
				return
			}
		}
		logger.debug("EXT023 fdat = " + fdat)

		// Check selection header
		DBAction EXT020_query = database.table("EXT020").index("00").build()
		DBContainer EXT020 = EXT020_query.getContainer()
		EXT020.set("EXCONO", currentCompany)
		EXT020.set("EXASCD", ascd)
		EXT020.set("EXCUNO", cuno)
		EXT020.setInt("EXFDAT", fdat as Integer)
		if(!EXT020_query.readAll(EXT020, 4, EXT020_outData)){
			String header = "MSG"
			String message = "Entête sélection n'existe pas"
			logMessage(header, message)
			return
		}

		DBAction EXT022_query = database.table("EXT022").index("00").selection("EXITNO").build()
		DBContainer EXT022 = EXT022_query.getContainer()
		EXT022.set("EXCONO", currentCompany)
		EXT022.set("EXASCD", ascd)
		EXT022.set("EXCUNO", cuno)
		EXT022.set("EXFDAT", fdat as Integer)
		if (!EXT022_query.readAll(EXT022, 4, EXT022_outData)) {
		}


		// Delete file EXTJOB
		deleteEXTJOB()
	}
	// Get first parameter
  private String getFirstParameter(){
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
  private String getNextParameter(){
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
	/**
	 * Delete records related to the current job from EXTJOB table
	 */
	public void deleteEXTJOB(){
		LocalDateTime timeOfCreation = LocalDateTime.now()
		DBAction query = database.table("EXTJOB").index("00").build()
		DBContainer EXTJOB = query.getContainer()
		EXTJOB.set("EXRFID", batch.getReferenceId().get())
		if(!query.readAllLock(EXTJOB, 1, updateCallBack_EXTJOB)){
		}
	}
	Closure<?> updateCallBack_EXTJOB = { LockedResult lockedResult ->
		lockedResult.delete()
	}
	void logMessage(String header, String message) {
		textFiles.open("FileImport")
		logFileName = "MSG_" + program.getProgramName() + "." + "batch" + "." + jobNumber + ".csv"
		if(!textFiles.exists(logFileName)) {
			log(header)
			log(message)
		}
	}
	void log(String message) {
		IN60 = true
		//logger.info(message)
		message = LocalDateTime.now().toString() + "" + message
		Closure<?> consumer = { PrintWriter printWriter ->
			printWriter.println(message)
		}
		textFiles.write(logFileName, "UTF-8", true, consumer)
	}
	Closure<?> EXT020_outData = { DBContainer EXT020 ->
	}
	Closure<?> EXT022_outData = { DBContainer EXT022 ->
		itno = EXT022.get("EXITNO")
		logger.debug("executeCRS105MIAddAssmItem : ascd = " + ascd)
		logger.debug("executeCRS105MIAddAssmItem : itno = " + itno)
		logger.debug("executeCRS105MIAddAssmItem : fdat = " + fdat)
		executeCRS105MIAddAssmItem(ascd, itno, fdat)
	}

	private executeCRS105MIAddAssmItem(String ASCD, String ITNO, String FDAT){
		def parameters = ["ASCD": ASCD, "ITNO": ITNO, "FDAT": FDAT]
		Closure<?> handler = { Map<String, String> response ->
			if (response.error != null) {
			} else {
			}
		}
		//Search Item exclusion
		exclu = false
		ExpressionFactory expression_EXT025 = database.getExpressionFactory("EXT025")
		expression_EXT025 = expression_EXT025.le("EXFDAT", FDAT)
		
		DBAction EXT025_query = database.table("EXT025").index("00").matching(expression_EXT025).selection("EXCONO", "EXITNO", "EXCUNO", "EXFDAT", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
		DBContainer EXT025 = EXT025_query.getContainer()
		EXT025.set("EXCONO", currentCompany)
		EXT025.set("EXCUNO", cuno)
		EXT025.set("EXITNO", ITNO)
		if(!EXT025_query.readAll(EXT025, 3, EXT025_outData)){
		}
		logger.debug("Exclu : " + ITNO + "-" + cuno +"-"+FDAT+"-"+exclu)
		if (!exclu) {
			miCaller.call("CRS105MI", "AddAssmItem", parameters, handler)
		}
	}
	Closure<?> EXT025_outData = { DBContainer EXT025 ->
		exclu = true
	}
}
