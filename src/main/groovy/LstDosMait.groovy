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
  private String fort_input
  private String tort_input
  private String frld_input
  private String trld_input
  private String whlo_input
  private String orno_input
  private String cunm
  private String cscd
  private Long znbc_DRADTR
  
  private String rout

  private String jobNumber

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
    whlo_input = (mi.in.get("WHLO") != null ? (String)mi.in.get("WHLO") : "")
    orno_input = (mi.in.get("ORNO") != null ? (String)mi.in.get("ORNO") : "")
    fort_input = (mi.in.get("FORT") != null ? (String)mi.in.get("FORT") : "")
    tort_input = (mi.in.get("TORT") != null ? (String)mi.in.get("TORT") : "")
    frld_input = (mi.in.get("FRLD") != null ? (String)mi.in.get("FRLD") : "")
    trld_input = (mi.in.get("TRLD") != null ? (String)mi.in.get("TRLD") : "")

    // check warehouse
    DBAction query_MITWHL = database.table("MITWHL").index("00").selection("MWWHLO").build()
    DBContainer MITWHL = query_MITWHL.getContainer()
    MITWHL.set("MWCONO", currentCompany)
    MITWHL.set("MWWHLO", whlo_input)
    if(!query_MITWHL.read(MITWHL)){
      mi.error("Le dépôt " + whlo_input + " n'existe pas")
      return
    }
    
    // Get OOLINE
    String tmpORNO = ""
    // TODO AMELIORER FILTRE
    logger.debug("ameliorer filtre")
    ExpressionFactory OOLINE_exp = database.getExpressionFactory("OOLINE")
    OOLINE_exp = OOLINE_exp.eq("OBWHLO", whlo_input)
    OOLINE_exp = OOLINE_exp.and(OOLINE_exp.lt("OBORST", "44"))

    DBAction OOLINE_query = database.table("OOLINE").index("00").matching(OOLINE_exp).selection("OBORNO","OBWHLO","OBROUT").build()
    DBContainer OOLINE_request = OOLINE_query.getContainer()
    OOLINE_request.set("OBCONO", currentCompany)
    
    Closure<?> OOLINE_reader = { DBContainer OOLINE_result ->
      
      String orno = OOLINE_result.get("OBORNO")
	    rout = OOLINE_result.get("OBROUT")
      if (orno != tmpORNO){
        getOOHEAD(orno)
        tmpORNO = orno
      }
	  
      
    }

    if(orno_input!="") {
      OOLINE_request.set("OBORNO", orno_input)
      if (!OOLINE_query.readAll(OOLINE_request, 2, OOLINE_reader)){
      }
    } else {
      if (!OOLINE_query.readAll(OOLINE_request, 1, OOLINE_reader)){
      }
    }
  }


  /**
   * Read OOHEAD data
   * @param orno
   * @return
   */
  public def getOOHEAD(String orno) {
    logger.debug("getoohead ${orno}")

    ExpressionFactory OOHEAD_exp = database.getExpressionFactory("OOHEAD")
    OOHEAD_exp = OOHEAD_exp.eq("OAUCA4", "")
    if(fort_input!="") {
      OOHEAD_exp = OOHEAD_exp.and(OOHEAD_exp.ge("OAORTP", fort_input))
    }
    if(tort_input!="") {
      OOHEAD_exp = OOHEAD_exp.and(OOHEAD_exp.le("OAORTP", tort_input))
    }
    if(frld_input!="") {
      OOHEAD_exp = OOHEAD_exp.and(OOHEAD_exp.ge("OARLDT", frld_input))
    }
    if(trld_input!="") {
      OOHEAD_exp = OOHEAD_exp.and(OOHEAD_exp.le("OARLDT", trld_input))
    }
	
	  OOHEAD_exp = OOHEAD_exp.and(OOHEAD_exp.ne("OAORTP","C20"))
    
    DBAction OOHEAD_query = database.table("OOHEAD").index("00").matching(OOHEAD_exp).selection(
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
        
    DBContainer OOHEAD_request = OOHEAD_query.getContainer()
    OOHEAD_request.set("OACONO", currentCompany)
    OOHEAD_request.set("OAORNO", orno)
    if (OOHEAD_query.read(OOHEAD_request)) {
      String ortp = OOHEAD_request.get("OAORTP") as String
      String cuno = OOHEAD_request.get("OACUNO") as String
      String cuor = OOHEAD_request.get("OACUOR") as String
      String rldt = OOHEAD_request.get("OARLDT") as String
      String resp = OOHEAD_request.get("OARESP") as String
      String uca4 = OOHEAD_request.get("OAUCA4") as String
      String uca5 = OOHEAD_request.get("OAUCA5") as String
      String uca6 = OOHEAD_request.get("OAUCA6") as String
      
      
      DBAction OCUSMA_query = database.table("OCUSMA").index("00").selection("OKCUNM","OKCSCD").build()
      DBContainer OCUSMA_request = OCUSMA_query.getContainer()
      OCUSMA_request.set("OKCONO",currentCompany)
      OCUSMA_request.set("OKCUNO",cuno)
      if (OCUSMA_query.read(OCUSMA_request)) {
        cunm = OCUSMA_request.get("OKCUNM") as String
        cscd = OCUSMA_request.get("OKCSCD") as String
      }
      //set output data
      mi.outData.put("ORNO", orno)
      mi.outData.put("ORTP", ortp)
      mi.outData.put("CUNO", cuno)
      mi.outData.put("CUNM", cunm)
      mi.outData.put("CUOR", cuor)
      mi.outData.put("CSCD", cscd)
      mi.outData.put("RLDT", rldt)
      mi.outData.put("RESP", resp)
      mi.outData.put("UCA4", uca4)
      mi.outData.put("UCA5", uca5)
      mi.outData.put("UCA6", uca6)
      mi.outData.put("ROUT", rout)
      mi.write()

    }
  }


}

