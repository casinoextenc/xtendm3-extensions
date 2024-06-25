/**
 * This extension is used by Mashup
 * Name : EXT023MI.UpdAssortItems
 * COMX01 Gestion des assortiments clients
 * Description : Read EXT022 table, delete items from the assortment that no longer apply (CRS105MI/DltAssmItem) and add new items (CRS105MI/AddAssmItem)
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 * 20240206     YVOYOU       COMX01 - Exclu item
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */
public class UpdAssortItems extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private String ascd
  private String cuno
  private String fdat
  private String itno
  private boolean exclu

  public UpdAssortItems(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    ascd = mi.in.get("ASCD")
    cuno = mi.in.get("CUNO")

    fdat = ""
    if (mi.in.get("FDAT") == null) {
      mi.error("Date de début est obligatoire")
      return
    } else {
      fdat = mi.in.get("FDAT")
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        mi.error("Date de début est invalide")
        return
      }
    }

    // Check selection header
    DBAction ext020Query = database.table("EXT020").index("00").build()
    DBContainer ext020Request = ext020Query.getContainer()
    ext020Request.set("EXCONO", currentCompany)
    ext020Request.set("EXASCD", ascd)
    ext020Request.set("EXCUNO", cuno)
    ext020Request.setInt("EXFDAT", fdat as Integer)
    if (!ext020Query.read(ext020Request)) {
      mi.error("Entête sélection n'existe pas")
      return
    }

    // Delete of non-selected items present in the assortment
    ExpressionFactory oasitnExpression = database.getExpressionFactory("OASITN")
    oasitnExpression = oasitnExpression.eq("OIFDAT", fdat)
    DBAction oasitnQuery = database.table("OASITN").index("00").matching(oasitnExpression).build()
    DBContainer oasitnRequest = oasitnQuery.getContainer()
    oasitnRequest.set("OICONO", currentCompany)
    oasitnRequest.set("OIASCD", ascd)
    if (!oasitnQuery.readAll(oasitnRequest, 2, 10000, oasitnReader)) {
    }

    // Add of selected items that are not in the assortment
    DBAction ext022Query = database.table("EXT022").index("00").selection("EXITNO").build()
    DBContainer ext022Request = ext022Query.getContainer()
    ext022Request.set("EXCONO", currentCompany)
    ext022Request.set("EXASCD", ascd)
    ext022Request.set("EXCUNO", cuno)
    ext022Request.set("EXFDAT", fdat as Integer)
    if (!ext022Query.readAll(ext022Request, 4, 10000, ext022Reader)) {
    }
  }

  Closure<?> ext022Reader = { DBContainer ext022Result ->
    itno = ext022Result.get("EXITNO")
    DBAction oasitnQuery = database.table("OASITN").index("00").build()
    DBContainer oasitnRequest = oasitnQuery.getContainer()
    oasitnRequest.set("OICONO", currentCompany)
    oasitnRequest.set("OIASCD", ascd)
    oasitnRequest.set("OIITNO", itno)
    oasitnRequest.set("OIFDAT", fdat as Integer)
    if (!oasitnQuery.read(oasitnRequest)) {
      //Search Item exclusion
      exclu = false
      ExpressionFactory ext025Expression = database.getExpressionFactory("EXT025")
      ext025Expression = ext025Expression.le("EXFDAT", fdat)

      DBAction ext025Query = database.table("EXT025").index("00").matching(ext025Expression).selection("EXCONO", "EXITNO", "EXCUNO", "EXFDAT", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer ext025Request = ext025Query.getContainer()
      ext025Request.set("EXCONO", currentCompany)
      ext025Request.set("EXCUNO", cuno)
      ext025Request.set("EXITNO", itno)
      if (!ext025Query.readAll(ext025Request, 3, 1, ext025Reader)) {
      }

      // Add assortment item
      if (!exclu) {
        executeCRS105MIAddAssmItem(ascd, itno, fdat)
      }
    }
  }
  Closure<?> ext025Reader = { DBContainer EXT025 ->
    exclu = true
  }

  Closure<?> oasitnReader = { DBContainer oasitnResult ->
    itno = oasitnResult.get("OIITNO")
    //Read corresponding record in EXT022
    DBAction ext022Query = database.table("EXT022").index("00").selection("EXITNO").build()
    DBContainer ext022Request = ext022Query.getContainer()
    ext022Request.set("EXCONO", currentCompany)
    ext022Request.set("EXASCD", ascd)
    ext022Request.set("EXCUNO", cuno)
    ext022Request.set("EXFDAT", fdat as Integer)
    ext022Request.set("EXITNO", itno)
    //if not exists delete in OASITN thru api
    if (!ext022Query.read(ext022Request)) {
      // Delete non-selected item from assortment
      executeCRS105MIDltAssmItem(ascd, itno, fdat)
    }
  }

  private void executeCRS105MIAddAssmItem(String ASCD, String ITNO, String FDAT) {
    Map<String, String> parameters = ["ASCD": ASCD, "ITNO": ITNO, "FDAT": FDAT]
    Closure<?> handler = { Map<String, String> response ->
    }
    miCaller.call("CRS105MI", "AddAssmItem", parameters, handler)
  }

  private void executeCRS105MIDltAssmItem(String ASCD, String ITNO, String FDAT) {
    Map<String, String> parameters = ["ASCD": ASCD, "ITNO": ITNO, "FDAT": FDAT]
    Closure<?> handler = { Map<String, String> response ->
    }
    miCaller.call("CRS105MI", "DltAssmItem", parameters, handler)
  }
}
