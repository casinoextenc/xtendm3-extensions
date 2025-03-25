/**
 * README
 * This extension is used by MEC
 *
 * Name : EXT062MI.AddOrdLineChrg
 * Description : Adds order line charge
 * Date         Changed By   Description
 * 20230821     RENARN       CMD03 - Calculation of service charges
 * 20240208     MLECLERCQ    CMD03 - Support PREX 6
 * 20240522     PBEAUDOUIN   Correction SUNO MPLINE quand RORC = 2
 * 20240809     YBLUTEAU     CMD03 - Prio 7 et SUNO Gold
 * 20241211     YJANNIN      CMD03 2.5 - Prio 7
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
public class AddOrdLineChrg extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany
  private String currentDate
  private String orno
  private String ponr
  private String posx
  private String orn2
  private String pon2
  private String pos2
  private String orst
  private String ortp
  private Integer chb3
  private Integer chb6
  private String cuno
  private String suno
  private String ordt
  private String hie1
  private String hie2
  private String hie3
  private String hie4
  private String hie5
  private String a830
  private String crid
  private String crfa
  private boolean recordFound
  private Integer rorc
  private Integer nbMaxRecord = 10000

  public AddOrdLineChrg(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
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

    // Get current date
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    // Original order
    orno = ""
    if(mi.in.get("ORNO") == null || mi.in.get("ORNO") == ""){
      mi.error("Numéro commande de vente est obligatoire")
      return
    } else {
      orno = mi.in.get("ORNO")
    }

    // Original order line
    ponr = ""
    if(mi.in.get("PONR") == null || mi.in.get("PONR") == ""){
      mi.error("Numéro ligne commande de vente est obligatoire")
      return
    } else {
      ponr = mi.in.get("PONR") as int
    }

    // Original order line suffix
    posx = "0"
    if(mi.in.get("POSX") != null && mi.in.get("POSX") != ""){
      posx = mi.in.get("POSX") as int
    }

    // Service charge order
    orn2 = ""
    if(mi.in.get("ORN2") != null && mi.in.get("ORN2") != ""){
      orn2 = mi.in.get("ORN2")
    }

    // Service charge order line
    pon2 = "0"
    if(mi.in.get("PON2") != null && mi.in.get("PON2") != ""){
      pon2 = mi.in.get("PON2") as int
    }

    // Service charge order line suffix
    pos2 = "0"
    if(mi.in.get("POS2") != null && mi.in.get("POS2") != ""){
      pos2 = mi.in.get("POS2") as int
    }

    // Check original order
    if(mi.in.get("ORNO") != null && mi.in.get("ORNO") != "") {
      ordt = ""
      DBAction queryOohead = database.table("OOHEAD").index("00").selection("OACUNO", "OAORST", "OAORTP", "OAORDT").build()
      DBContainer requestOohead = queryOohead.getContainer()
      requestOohead.set("OACONO", currentCompany)
      requestOohead.set("OAORNO", mi.in.get("ORNO"))
      if (queryOohead.read(requestOohead)) {
        orst = requestOohead.get("OAORST")

        ortp = requestOohead.get("OAORTP")
        // Check order type
        chb3 = 0
        DBAction queryCugex1 = database.table("CUGEX1").index("00").selection("F1CHB3", "F1CHB6").build()
        DBContainer requestCugex1 = queryCugex1.getContainer()
        requestCugex1.set("F1CONO", currentCompany)
        requestCugex1.set("F1FILE", "OOTYPE")
        requestCugex1.set("F1PK01", requestOohead.get("OAORTP"))
        requestCugex1.set("F1PK02", "")
        requestCugex1.set("F1PK03", "")
        requestCugex1.set("F1PK04", "")
        requestCugex1.set("F1PK05", "")
        requestCugex1.set("F1PK06", "")
        requestCugex1.set("F1PK07", "")
        requestCugex1.set("F1PK08", "")
        if (queryCugex1.read(requestCugex1)) {
          chb3 = requestCugex1.get("F1CHB3")
        }
        if (chb3 == 0) {
          mi.error("ORNO - Type de commande " + requestOohead.get("OAORTP") + " est invalide")
          return
        }
        // Check customer setting
        if (orn2.trim() == "") {
          chb6 = 0
          requestCugex1.set("F1CONO", currentCompany)
          requestCugex1.set("F1FILE", "OCUSMA")
          requestCugex1.set("F1PK01", requestOohead.get("OACUNO"))
          requestCugex1.set("F1PK02", "")
          requestCugex1.set("F1PK03", "")
          requestCugex1.set("F1PK04", "")
          requestCugex1.set("F1PK05", "")
          requestCugex1.set("F1PK06", "")
          requestCugex1.set("F1PK07", "")
          requestCugex1.set("F1PK08", "")
          if (queryCugex1.read(requestCugex1)) {
            chb6 = requestCugex1.get("F1CHB6")
          }

          if (chb6 == 1) {
            return
          }
        }
        ordt = requestOohead.get("OAORDT") as String
      } else {
        mi.error("ORNO - Numéro de commande " + mi.in.get("ORNO") + " n'existe pas")
        return
      }
    }

    // Check Service charge order
    if(mi.in.get("ORN2") != null && mi.in.get("ORN2") != "") {
      DBAction queryOohead = database.table("OOHEAD").index("00").selection("OACUNO", "OAORST", "OAORTP").build()
      DBContainer requestOohead = queryOohead.getContainer()
      requestOohead.set("OACONO", currentCompany)
      requestOohead.set("OAORNO", mi.in.get("ORN2"))
      if (queryOohead.read(requestOohead)) {
        orst = requestOohead.get("OAORST")
        // Check order status
        if (orst.trim() > "69") {
          mi.error("ORN2 - Statut supérieur commande de vente " + orst + " est invalide")
          return
        }
        // Check order type
        chb3 = 0
        DBAction queryCugex1 = database.table("CUGEX1").index("00").selection("F1CHB3", "F1CHB6").build()
        DBContainer requestCugex1 = queryCugex1.getContainer()
        requestCugex1.set("F1CONO", currentCompany)
        requestCugex1.set("F1FILE", "OOTYPE")
        requestCugex1.set("F1PK01", requestOohead.get("OAORTP"))
        requestCugex1.set("F1PK02", "")
        requestCugex1.set("F1PK03", "")
        requestCugex1.set("F1PK04", "")
        requestCugex1.set("F1PK05", "")
        requestCugex1.set("F1PK06", "")
        requestCugex1.set("F1PK07", "")
        requestCugex1.set("F1PK08", "")
        if (queryCugex1.read(requestCugex1)) {
          chb3 = requestCugex1.get("F1CHB3")
        }
      } else {
        mi.error("ORN2 - Numéro de commande " + mi.in.get("ORN2") + " n'existe pas")
        return
      }
    }

    // Retrieve order line
    cuno = ""
    suno = ""
    hie1 = ""
    hie2 = ""
    hie3 = ""
    hie4 = ""
    hie5 = ""
    a830 = ""
    DBAction queryOoline = database.table("OOLINE").index("00").selection("OBCUNO","OBSUNO","OBITNO","OBRORC","OBRORN","OBRORL","OBRORX").build()
    DBContainer requestOoline = queryOoline.getContainer()
    requestOoline.set("OBCONO", currentCompany)
    requestOoline.set("OBORNO", mi.in.get("ORNO")) // Original order
    requestOoline.set("OBPONR", mi.in.get("PONR")) // Original order line
    requestOoline.set("OBPOSX", mi.in.get("POSX")) // Original order line suffix
    if(queryOoline.read(requestOoline)){
      cuno = requestOoline.get("OBCUNO")
      suno = requestOoline.get("OBSUNO")
      rorc = requestOoline.get("OBRORC")
      String rorn = requestOoline.get("OBRORN") as String
      int rorl = requestOoline.get("OBRORL") as Integer
      int rorx = requestOoline.get("OBRORX") as Integer

      if(rorc == 2 && rorn != ""){
        DBAction queryMpline = database.table("MPLINE").index("00").selection("IBSUNO", "IBWHLO", "IBSUDO", "IBDNDT").build()
        DBContainer requestMline = queryMpline.getContainer()
        requestMline.set("IBCONO", currentCompany)
        requestMline.set("IBPUNO", rorn)
        requestMline.set("IBPNLI", rorl)
        requestMline.set("IBPNLS", rorx)
        if(queryMpline.read(requestMline)){
          suno = requestMline.get("IBSUNO").toString()
          String whlo = requestMline.get("IBWHLO") as String
          String sudo = requestMline.get("IBSUDO") as String
          int dndt = requestMline.get("IBDNDT") as Integer
          if(sudo != "") {
            DBAction queryPdnhea = database.table("PDNHEA").index("00").selection("IHCFK5").build()
            DBContainer requestPdnhea = queryPdnhea.getContainer()
            requestPdnhea.set("IHCONO", currentCompany)
            requestPdnhea.set("IHWHLO", whlo)
            requestPdnhea.set("IHSUNO", suno)
            requestPdnhea.set("IHSUDO", sudo)
            requestPdnhea.set("IHDNDT", dndt)
            if(queryPdnhea.read(requestPdnhea)){
              suno = requestPdnhea.get("IHCFK5").toString()
            }
          }
        }
      }

      // Retrieve item
      DBAction queryMitmas = database.table("MITMAS").index("00").selection("MMHIE1","MMHIE2","MMHIE3","MMHIE4","MMHIE5").build()
      DBContainer requestMitmas = queryMitmas.getContainer()
      requestMitmas.set("MMCONO", currentCompany)
      requestMitmas.set("MMITNO", requestOoline.get("OBITNO"))
      if(queryMitmas.read(requestMitmas)){
        hie1 = requestMitmas.get("MMHIE1")
        hie2 = requestMitmas.get("MMHIE2")
        hie3 = requestMitmas.get("MMHIE3")
        hie4 = requestMitmas.get("MMHIE4")
        hie5 = requestMitmas.get("MMHIE5")
      }

      DBAction qCugex1 = database.table("CUGEX1").index("00").selection("F1A830").build()
      DBContainer rCugex1 = qCugex1.getContainer()
      rCugex1.set("F1CONO", currentCompany)
      rCugex1.set("F1FILE", "MITMAS")
      rCugex1.set("F1PK01", requestOoline.get("OBITNO"))
      rCugex1.set("F1PK02", "")
      rCugex1.set("F1PK03", "")
      rCugex1.set("F1PK04", "")
      rCugex1.set("F1PK05", "")
      rCugex1.set("F1PK06", "")
      rCugex1.set("F1PK07", "")
      rCugex1.set("F1PK08", "")
      if (qCugex1.read(rCugex1)) {
        a830 = rCugex1.get("F1A830")
      }

    } else {
      mi.error("Ligne de commande " + ponr + " n'existe pas")
      return
    }

    recordFound = false
    ExpressionFactory expression = database.getExpressionFactory("EXT061")
    expression = expression.le("EXVFDT", ordt)
    expression = expression.and((expression.ge("EXLVDT", ordt)).or(expression.eq("EXLVDT", "0")))
    DBAction queryExt061 = database.table("EXT061").index("00").matching(expression).selection("EXCRID", "EXCRFA").build()
    DBContainer requestExt061 = queryExt061.getContainer()
    // Read selected charges
    requestExt061.set("EXCONO", currentCompany)
    requestExt061.set("EXPREX", " 1")
    requestExt061.set("EXOBV1", cuno.trim())
    requestExt061.set("EXOBV2", a830.trim())
    requestExt061.set("EXOBV3", suno.trim())
    requestExt061.set("EXOBV4", hie3.trim())
    if (!queryExt061.readAll(requestExt061, 6, nbMaxRecord, outDataExt061)) {
      requestExt061.set("EXPREX", " 2")
      requestExt061.set("EXOBV4", hie2.trim())
      if (!queryExt061.readAll(requestExt061, 6, nbMaxRecord, outDataExt061)) {
        requestExt061.set("EXPREX", " 3")
        requestExt061.set("EXOBV4", hie1.trim())
        if (!queryExt061.readAll(requestExt061, 6, nbMaxRecord, outDataExt061)) {
          requestExt061.set("EXPREX", " 4")
          requestExt061.set("EXOBV4", "")
          if (!queryExt061.readAll(requestExt061, 6, nbMaxRecord, outDataExt061)) {
            requestExt061.set("EXPREX", " 5")
            requestExt061.set("EXOBV3", "")
            if (!queryExt061.readAll(requestExt061, 6, nbMaxRecord, outDataExt061)) {
              requestExt061.set("EXPREX", " 6")
              requestExt061.set("EXOBV2", "")
              if (!queryExt061.readAll(requestExt061, 6, nbMaxRecord, outDataExt061)) {
              }
            }
          }
        }
      }
    }
    if(!recordFound){
      return
    }
  }
  // Retrieve EXT061
  Closure<?> outDataExt061 = { DBContainer resultExt061 ->
    recordFound = true
    crid = resultExt061.get("EXCRID")
    crfa = resultExt061.get("EXCRFA")
    if(orn2.trim() == "") {
      executeOIS100MIAddLineCharge(orno, ponr, posx, crid, crfa)
    } else {
      executeOIS100MIAddLineCharge(orn2, pon2, pos2, crid, crfa)
    }
  }
  // Execute OIS100MI.AddLineCharge
  private executeOIS100MIAddLineCharge(String pOrno, String pPonr, String pPosx, String pCrid, String pCrfa){
    Map<String, String> parameters = ["ORNO": pOrno, "PONR": pPonr, "POSX": pPosx, "CRID": pCrid, "CRFA": pCrfa]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Erreur OIS100MI AddLineCharge: "+ response.errorMessage)
      } else {
      }
    }
    miCaller.call("OIS100MI", "AddLineCharge", parameters, handler)
  }
}
