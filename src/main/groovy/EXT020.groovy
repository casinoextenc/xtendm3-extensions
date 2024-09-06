import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Name : EXT020
 * COMX01 Gestion des assortiments clients
 * Description : Select the items based on the criteria contained in EXT021 table and add records to the EXT020 table (EXT020MI.SelAssortItems conversion)
 * Date         Changed By   Description
 * 20240118     YYOU         COMX01 - Submit calc EXT022
 */

public class EXT020 extends ExtendM3Batch {
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private final BatchAPI batch

  private int currentCompany
  private String currentDivision
  private String cuno = ""
  private String svcuno = ""
  private String fvdt = ""
  private String fdat = ""
  private String todaydat = ""
  private String ascd = ""
  private String rawData
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private String jobNumber

  public EXT020(LoggerAPI logger, DatabaseAPI database, UtilityAPI utility, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles) {
    this.logger = logger
    this.database = database
    this.program = program
    this.batch = batch
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {
    // Get job number
    LocalDateTime timeOfCreation = LocalDateTime.now()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    if (batch.getReferenceId().isPresent()) {
      Optional<String> data = getJobData(batch.getReferenceId().get())
      if (!data.isEmpty())
        performActualJob(data)
    } else {
      // No job data found
    }
  }

  // Get job data
  private Optional<String> getJobData(String referenceId) {
    def extjobQuery = database.table("EXTJOB").index("00").selection("EXDATA").build()
    def extjobRequest = extjobQuery.createContainer()
    extjobRequest.set("EXRFID", referenceId)
    if (extjobQuery.read(extjobRequest)) {
      return Optional.of(extjobRequest.getString("EXDATA"))
    } else {
    }
    return Optional.empty()
  }

  /**
   * @param data
   * @return
   */
  private performActualJob(Optional<String> data) {
    if (!data.isPresent()) {
      return
    }
    rawData = data.get()
    String inNBDAYS = getFirstParameter()

    svcuno = ""

    currentCompany = (Integer) program.getLDAZD().CONO
    currentDivision = program.getLDAZD().DIVI
    LocalDateTime timeOfCreation = LocalDateTime.now()

    if (inNBDAYS != null && !inNBDAYS.trim().isBlank()) {
      LocalDate currentDate = LocalDate.now()

      // Subtract 7 days from the current date
      LocalDate dateMinus7Days = currentDate.minusDays(inNBDAYS as int)
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
      fdat = dateMinus7Days.format(formatter)
      todaydat = currentDate.format(formatter)
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        return
      }
    } else {
      fdat = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    }

    //Read EXT010 modified since fdat
    ExpressionFactory ext010Expression = database.getExpressionFactory("EXT010")
    ext010Expression = ext010Expression.ge("EXLMDT", fdat)
    DBAction ext010Query = database.table("EXT010").index("02").matching(ext010Expression).selection("EXCUNO").build()
    DBContainer ext010Request = ext010Query.getContainer()
    ext010Request.set("EXCONO", currentCompany)
    //no limitations in readall we have to treat all records update where LMDT > fdat
    if (!ext010Query.readAll(ext010Request, 1, ext010Reader)) {
    }
  }

  // Read EXT010
  Closure<?> ext010Reader = { DBContainer ext010Result ->
    cuno = ext010Result.getString("EXCUNO").trim()
    String[] tbcuno = ["90150", "90153", "90154", "90156", "90158", "97041"]// todo test remove
    if (cuno != svcuno && cuno in tbcuno) {// todo test remove
    //if (cuno != svcuno) {// todo test reactivate
      ExpressionFactory oascusExpression = database.getExpressionFactory("OASCUS")
      oascusExpression = oascusExpression.le("OCFDAT", todaydat)
      oascusExpression = oascusExpression.and(oascusExpression.ge("OCTDAT", todaydat))
      DBAction oascusQuery = database.table("OASCUS").index("20").matching(oascusExpression).selection("OCCONO", "OCASCD", "OCCUNO", "OCFDAT").build()
      DBContainer oascusRequest = oascusQuery.getContainer()
      oascusRequest.set("OCCONO", currentCompany)
      ascd = ext010Result.get("EXCUNO") as String
      oascusRequest.set("OCCUNO", cuno)
      if (!oascusQuery.readAll(oascusRequest, 2, 500, oascusReader)) {
      }
    }
    svcuno = cuno
  }

// Read OASCUS for each record we post
  Closure<?> oascusReader = { DBContainer oascusResult ->
    // Add selected items in the assortment
    fvdt = oascusResult.get("OCFDAT") as String
    ascd = oascusResult.get("OCASCD")
    cuno = oascusResult.get("OCCUNO")
    executeEXT820MISubmitBatch(currentCompany as String, "EXT022", ascd, cuno, fvdt, "", "2", "", "", "")
  }

// Execute EXT820MI.SubmitBatch
  private executeEXT820MISubmitBatch(String CONO, String JOID, String P001, String P002, String P003, String P004, String P005, String P006, String P007, String P008) {
    def parameters = ["CONO": CONO, "JOID": JOID, "P001": P001, "P002": P002, "P003": P003, "P004": P004, "P005": P005, "P006":
      P006, "P007"          : P007, "P008": P008]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      } else {
      }
    }
    miCaller.call("EXT820MI", "SubmitBatch", parameters, handler)
  }

// Get first parameter
  private String getFirstParameter() {
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
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
    return parameter
  }
}
