/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT054MI.AddItem
 * Description : Adds the item received as a parameter in the assortments and price lists
 * Date         Changed By   Description
 * 20210503     RENARN       TARX02 - Add assortment
 * 20220214     RENARN       EXT082MI.MngPrcLst has been replaced by EXT820MI.SubmitBatch
 */
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
public class AddItem extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller;
  private final ProgramAPI program;
  private final UtilityAPI utility;
  private int currentCompany
  private String ascd
  private String cuno
  private String fdat
  private String itno
  private String prrf
  private String cucd
  private String fvdt

  public AddItem(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi;
    this.database = database;
    this.logger = logger;
    this.program = program;
    this.utility = utility;
    this.miCaller = miCaller
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO;
    } else {
      currentCompany = mi.in.get("CONO");
    }

    // Check item
    itno = mi.in.get("ITNO")
    if(mi.in.get("ITNO") != null){
      DBAction query = database.table("MITMAS").index("00").build()
      DBContainer MITMAS = query.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO",  mi.in.get("ITNO"))
      if (!query.read(MITMAS)) {
        mi.error("Code article " + mi.in.get("ITNO") + " n'existe pas")
        return
      }
    } else {
      mi.error("Code article est obligatoire")
      return
    }

    // Add item to the assortments
    LocalDateTime timeOfCreation = LocalDateTime.now();
    ExpressionFactory expression = database.getExpressionFactory("EXT050");
    expression = expression.le("EXDAT1", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    DBAction query = database.table("EXT050").index("00").matching(expression).selection("EXCONO", "EXASCD", "EXCUNO", "EXDAT1", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build();
    DBContainer EXT050 = query.getContainer();
    EXT050.setInt("EXCONO",currentCompany);
    if(!query.readAll(EXT050, 1, EXT050_outData)){
      mi.error("L'enregistrement n'existe pas");
      return;
    }

    // Add item to the price list
    ExpressionFactory EXT080_expression = database.getExpressionFactory("EXT080");
    EXT080_expression = EXT080_expression.le("EXFVDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    EXT080_expression = EXT080_expression.and(EXT080_expression.ge("EXLVDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))));
    DBAction EXT080_query = database.table("EXT080").index("00").matching(EXT080_expression).selection("EXCONO", "EXPRRF", "EXCUCD", "EXCUNO", "EXFVDT").build();
    DBContainer EXT080 = EXT080_query.getContainer();
    EXT080.setInt("EXCONO",currentCompany);
    if(!EXT080_query.readAll(EXT080, 1, EXT080_outData)){
      mi.error("L'enregistrement n'existe pas");
      return;
    }
  }
  Closure<?> EXT050_outData = { DBContainer EXT050 ->
    ascd = EXT050.get("EXASCD")
    cuno = EXT050.get("EXCUNO")
    fdat = EXT050.get("EXDAT1")
    logger.debug("logger EXT054MI EXT050_outData : ascd = " + ascd)
    logger.debug("logger EXT054MI EXT050_outData : cuno = " + cuno)
    logger.debug("logger EXT054MI EXT050_outData : fdat = " + fdat)
    logger.debug("logger EXT054MI EXT050_outData : itno = " + itno)
    // If the item matches the selection, it is added to the assortment
    executeEXT052MISelAssortItems(ascd, cuno, fdat, itno)
  }
  Closure<?> EXT080_outData = { DBContainer EXT080 ->
    prrf = EXT080.get("EXPRRF")
    cucd = EXT080.get("EXCUCD")
    cuno = EXT080.get("EXCUNO")
    fvdt = EXT080.get("EXFVDT")
    logger.debug("logger EXT054MI EXT080_outData : prrf = " + prrf)
    logger.debug("logger EXT054MI EXT080_outData : cucd = " + cucd)
    logger.debug("logger EXT054MI EXT080_outData : cuno = " + cuno)
    logger.debug("logger EXT054MI EXT080_outData : fvdt = " + fvdt)
    // If the item matches the selection, it is added to the price list
    executeEXT820MISubmitBatch(currentCompany as String, "EXT082", prrf, cucd, cuno, fvdt, "1", itno)
  }
  private executeEXT052MISelAssortItems(String ASCD, String CUNO, String FDAT, String ITNO){
    def parameters = ["ASCD": ASCD, "CUNO": CUNO, "FDAT": FDAT, "ITNO": ITNO]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      } else {
      }
    }
    miCaller.call("EXT052MI", "SelAssortItems", parameters, handler)
  }
  private executeEXT820MISubmitBatch(String CONO, String JOID, String P001, String P002, String P003, String P004, String P005, String P006){
    def parameters = ["CONO": CONO, "JOID": JOID, "P001": P001, "P002": P002, "P003": P003, "P004": P004, "P005": P005, "P006": P006]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      } else {
      }
    }
    miCaller.call("EXT820MI", "SubmitBatch", parameters, handler)
  }
}
