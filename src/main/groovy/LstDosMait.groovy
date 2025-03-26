/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstDosMait
 * Description : batch template
 * Date         Changed By   Description
 * 20230511     SEAR         LOG28 - Creation of files and containers
 * 20230811     MLECLERCQ    LOG28 - bugs correction
 * 20230818     MLECLERCQ    LOG28 - REJECT ORTP = C20
 * 20230818     MLECLERCQ    LOG28 - ROUT from OOLINE instead of OOHEAD
 * 20230818     MLECLERCQ    LOG28 - Corrected FRLD instead of FLRD in Mi Inputs
 * 20240430     MLECLERCQ    LOG28 - added country name
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class LstDosMait extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  int currentCompany

  private String parm
  private String fortInput
  private String tortInput
  private String frldInput
  private String trldInput
  private String whloInput
  private String ornoInput
  private String cunoInput
  private String cunm
  private String cscd
  private String cscn
  private Long znbcDradtr

  private String rout
  private String massification

  private String jobNumber
  private Integer nbMaxRecord = 10000

  public LstDosMait(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.logger = logger
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentCompany = (Integer)program.getLDAZD().CONO

    //Get mi inputs
    whloInput = (mi.in.get("WHLO") != null ? (String)mi.in.get("WHLO") : "")
    ornoInput = (mi.in.get("ORNO") != null ? (String)mi.in.get("ORNO") : "")
    fortInput = (mi.in.get("FORT") != null ? (String)mi.in.get("FORT") : "")
    tortInput = (mi.in.get("TORT") != null ? (String)mi.in.get("TORT") : "")
    frldInput = (mi.in.get("FRLD") != null ? (String)mi.in.get("FRLD") : "")
    trldInput = (mi.in.get("TRLD") != null ? (String)mi.in.get("TRLD") : "")
    cunoInput = (mi.in.get("CUNO") != null ? (String)mi.in.get("CUNO") : "")

    // check warehouse
    DBAction queryMitwhl = database.table("MITWHL").index("00").selection("MWWHLO").build()
    DBContainer MITWHL = queryMitwhl.getContainer()
    MITWHL.set("MWCONO", currentCompany)
    MITWHL.set("MWWHLO", whloInput)
    if(!queryMitwhl.read(MITWHL)){
      mi.error("Le dépôt " + whloInput + " n'existe pas")
      return
    }

    if(cunoInput){
      // check Customer
      ExpressionFactory ocusmaExp = database.getExpressionFactory("OCUSMA")
      ocusmaExp = ocusmaExp.eq("OKSTAT", "20")
      DBAction queryOcusma = database.table("OCUSMA").index("00").matching(ocusmaExp).selection("OKCUNO").build()
      DBContainer OCUSMA = queryOcusma.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", cunoInput)
      if(!queryOcusma.read(OCUSMA)){
        mi.error("Client " + cunoInput + " n'existe pas ou statut non valide")
        return
      }
    }

    // Get OOLINE
    String tmpORNO = ""
    // TODO AMELIORER FILTRE
    logger.debug("ameliorer filtre")
    ExpressionFactory oolineExp = database.getExpressionFactory("OOLINE")
    oolineExp = oolineExp.eq("OBWHLO", whloInput)
    oolineExp = oolineExp.and(oolineExp.lt("OBORST", "44"))

    DBAction oolineQuery = database.table("OOLINE").index("00").matching(oolineExp).selection("OBORNO","OBWHLO","OBROUT").build()
    DBContainer oolineRequest = oolineQuery.getContainer()
    oolineRequest.set("OBCONO", currentCompany)

    Closure<?> oolineReader = { DBContainer oolineResult ->

      String orno = oolineResult.get("OBORNO")
      rout = oolineResult.get("OBROUT")
      if (orno != tmpORNO){
        getOOHEAD(orno)
        tmpORNO = orno
      }

    }

    if(ornoInput!="") {
      oolineRequest.set("OBORNO", ornoInput)
      if (!oolineQuery.readAll(oolineRequest, 2, nbMaxRecord, oolineReader)){
      }
    } else {
      if (!oolineQuery.readAll(oolineRequest, 1, nbMaxRecord, oolineReader)){
      }
    }
  }

  /**
   * Read OOHEAD data
   * @param orno
   * @return
   */
  public void getOOHEAD(String orno) {
    logger.debug("getoohead ${orno}")

    ExpressionFactory ooheadExp = database.getExpressionFactory("OOHEAD")
    ooheadExp = ooheadExp.eq("OAUCA4", "")
    if(fortInput!="") {
      ooheadExp = ooheadExp.and(ooheadExp.ge("OAORTP", fortInput))
    }
    if(tortInput!="") {
      ooheadExp = ooheadExp.and(ooheadExp.le("OAORTP", tortInput))
    }
    if(frldInput!="") {
      ooheadExp = ooheadExp.and(ooheadExp.ge("OARLDT", frldInput))
    }
    if(trldInput!="") {
      ooheadExp = ooheadExp.and(ooheadExp.le("OARLDT", trldInput))
    }

    if(cunoInput!="") {
      ooheadExp = ooheadExp.and(ooheadExp.eq("OACUNO", cunoInput))
    }

    ooheadExp = ooheadExp.and(ooheadExp.ne("OAORTP","C20"))
    ooheadExp = ooheadExp.and(ooheadExp.ge("OAORSL","20"))

    DBAction ooheadQuery = database.table("OOHEAD").index("00").matching(ooheadExp).selection(
      "OACONO"
      ,"OAORNO"
      ,"OAORTP"
      ,"OARLDT"
      ,"OACUNO"
      ,"OACUOR"
      ,"OARESP"
      ,"OACUOR"
      ,"OAUCA4"
      ,"OAUCA5"
      ,"OAUCA6"
    ).build()

    DBContainer ooheadRequest = ooheadQuery.getContainer()
    ooheadRequest.set("OACONO", currentCompany)
    ooheadRequest.set("OAORNO", orno)
    if (ooheadQuery.read(ooheadRequest)) {
      String ortp = ooheadRequest.get("OAORTP") as String
      String cuno = ooheadRequest.get("OACUNO") as String
      String cuor = ooheadRequest.get("OACUOR") as String
      String rldt = ooheadRequest.get("OARLDT") as String
      String resp = ooheadRequest.get("OARESP") as String
      String uca4 = ooheadRequest.get("OAUCA4") as String
      String uca5 = ooheadRequest.get("OAUCA5") as String
      String uca6 = ooheadRequest.get("OAUCA6") as String

      DBAction ocusmaQuery = database.table("OCUSMA").index("00").selection("OKCUNM","OKCSCD").build()
      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO",currentCompany)
      ocusmaRequest.set("OKCUNO",cuno)
      if (ocusmaQuery.read(ocusmaRequest)) {
        cunm = ocusmaRequest.get("OKCUNM") as String
        cscd = ocusmaRequest.get("OKCSCD") as String
        getCountryName(cscd)
        getMassification(cuno, rldt, orno)
      }
      //set output data
      mi.outData.put("ORNO", orno)
      mi.outData.put("ORTP", ortp)
      mi.outData.put("CUNO", cuno)
      mi.outData.put("CUNM", cunm)
      mi.outData.put("CUOR", cuor)
      mi.outData.put("CSCD", cscd)
      mi.outData.put("CSCN",cscn)
      mi.outData.put("RLDT", rldt)
      mi.outData.put("RESP", resp)
      mi.outData.put("UCA4", uca4)
      mi.outData.put("UCA5", uca5)
      mi.outData.put("UCA6", uca6)
      mi.outData.put("ROUT", rout)
      mi.outData.put("MASS", massification)
      mi.write()

    }
  }

  /**
   * Get country name
   * @param countryCode
   * @return
   */
  private getCountryName(countryCode){
    DBAction csytabQuery = database.table("CSYTAB").index("20").selection("CTTX40").build()
    DBContainer csytabRequest = csytabQuery.getContainer()
    csytabRequest.set("CTCONO", currentCompany)
    csytabRequest.set("CTSTCO", 'CSCD')
    csytabRequest.set("CTSTKY", countryCode)

    csytabQuery.readAll(csytabRequest, 3,1, { DBContainer csytabClosure ->
      cscn = csytabClosure.get("CTTX40") as String
    })
  }

  /**
   * Get massification
   * @param cuno
   * @param livDate
   * @param orno
   * @return
   */
  private getMassification(cuno, livDate,orno){
    ExpressionFactory exprExt014 = database.getExpressionFactory("EXT014")

    exprExt014 = exprExt014.le("EXFVDT",livDate.toString())
    exprExt014 = exprExt014.and(exprExt014.ge("EXLVDT",livDate.toString()))

    DBAction ext014Query = database.table("EXT014").index("00").matching(exprExt014).selection("EXCONO","EXCUNO","EXWHLO","EXFVDT","EXLVDT").build()
    DBContainer ext014Request = ext014Query.getContainer()
    ext014Request.set("EXCONO", currentCompany)
    ext014Request.set("EXCUNO", cuno)
    ext014Request.set("EXWHLO",whloInput)

    logger.debug("in getMassification for orno : ${orno.toString()}")

    if (!ext014Query.readAll(ext014Request,3,1,{})) {
      massification = "0"
    }else{
      massification = "1"
    }
  }
}
