import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Name : EXT105MI.AddAssFile
 *
 * Description :
 * Create records in EXT105
 *  We don't control inputs
 *  if customer doesn't exists or if datas are wrong we store record in EXT105 with STAT = 75 and TXER = error message
 * Date         Changed By    Version   Description
 * 20250729     FLEBARS       1.0       Creation
 */

public class AddAssFile extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private String currentDate


  public AddAssFile(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    currentCompany = (int) program.getLDAZD().CONO
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    boolean errorFlag = false
    String errorMessage = ""

    String cuno = (String) mi.in.get("CUNO")
    String trdt = (String) mi.in.get("TRDT")
    String file = (String) mi.in.get("FILE")

    logger.debug("TRDT " + trdt)


    if (!utility.call("DateUtil", "isDateValid", trdt, "yyyyMMdd")) {
      mi.error("Date est invalide")
      return
    }

    //Check customer datas
    if (cuno != null) {
      DBAction ocusmaQuery = database.table("OCUSMA")
        .index("00")
        .selection("OKCUTP", "OKSTAT")
        .build()
      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO", currentCompany)
      ocusmaRequest.set("OKCUNO", cuno)
      if (!ocusmaQuery.read(ocusmaRequest)) {
        errorMessage = "Code Client " + cuno + " n'existe pas"
        errorFlag = true
      }
      if (!errorFlag && ocusmaRequest.get("OKSTAT") != "20") {
        errorMessage = "Statut Client " + cuno + " est invalide"
        errorFlag = true
      }
      if (!errorFlag && ocusmaRequest.getInt("OKCUTP") != 0) {
        errorMessage = "Type de Client " + cuno + " est invalide"
        errorFlag = true
      }
    } else {
      mi.error("Code Client est obligatoire")
      return
    }
    //Check price table
    String prrf = getPriceLstMatrix(cuno)

    if (prrf.isEmpty()) {
      errorMessage = "Code tarif inexistant dans OIS931"
      errorFlag = true
    } else {
      ExpressionFactory oprichExpression = database.getExpressionFactory("OPRICH")
      oprichExpression = oprichExpression.le("OJFVDT", currentDate)
      oprichExpression = oprichExpression.and(oprichExpression.ge("OJLVDT", currentDate))

      DBAction oprichQuery = database.table("OPRICH")
        .index("00")
        .matching(oprichExpression)
        .selection(
          "OJFVDT",
          "OJLVDT",
          "OJTX40",
          "OJTX15",
          "OJCRTP"
        )
        .build()

      DBContainer oprichRequest = oprichQuery.getContainer()
      oprichRequest.set("OJCONO", currentCompany)
      oprichRequest.set("OJPRRF", prrf)
      oprichRequest.set("OJCUCD", "EUR")
      oprichRequest.set("OJCUNO", cuno)

      Closure<?> oprichReader = { DBContainer oprichRecord ->
      }
      if (!oprichQuery.readAll(oprichRequest, 4, 1, oprichReader)) {
        errorMessage = "Code tarif inexistant dans OIS017"
        errorFlag = true
      }
    }

    DBAction ext105Query = database.table("EXT105").index("00").build()
    DBContainer ext105Request = ext105Query.getContainer()
    ext105Request.set("EXCONO", currentCompany)
    ext105Request.set("EXCUNO", cuno)
    ext105Request.setInt("EXTRDT", trdt as Integer)
    ext105Request.set("EXFILE", file)
    if (!ext105Query.read(ext105Request)) {
      ext105Request.set("EXSTAT", errorFlag ? "75" : "00")
      ext105Request.set("EXTXER", errorMessage)
      ext105Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext105Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      ext105Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext105Request.setInt("EXCHNO", 1)
      ext105Request.set("EXCHID", program.getUser())
      ext105Query.insert(ext105Request)
    } else {
      mi.error("L'enregistrement existe déjà")
      return
    }

  }

  /**
   * Get price list matrix for customer
   * @return prrf
   */
  private String getPriceLstMatrix(String cuno) {
    String oPrrf = ""
    // Delete Assortment selection
    DBAction oprmtxQuery = database.table("OPRMTX")
      .index("00")
      .selection("DXPRRF")
      .build()
    DBContainer oprmtxRequest = oprmtxQuery.getContainer()
    oprmtxRequest.set("DXCONO", currentCompany)
    oprmtxRequest.set("DXPLTB", "CSN")
    oprmtxRequest.set("DXPREX", " 5")
    oprmtxRequest.set("DXOBV1", cuno.trim())
    if (oprmtxQuery.read(oprmtxRequest)) {
      oPrrf = oprmtxRequest.get("DXPRRF") as String
    }
    return oPrrf.trim()
  }



}
