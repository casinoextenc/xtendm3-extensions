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
      DBAction queryOOHEAD = database.table("OOHEAD").index("00").selection("OACUNO", "OAORST", "OAORTP", "OAORDT").build()
      DBContainer OOHEAD = queryOOHEAD.getContainer()
      OOHEAD.set("OACONO", currentCompany)
      OOHEAD.set("OAORNO", mi.in.get("ORNO"))
      if (queryOOHEAD.read(OOHEAD)) {
        orst = OOHEAD.get("OAORST")

        ortp = OOHEAD.get("OAORTP")
        // Check order type
        chb3 = 0
        DBAction queryCUGEX1 = database.table("CUGEX1").index("00").selection("F1CHB3", "F1CHB6").build()
        DBContainer CUGEX1 = queryCUGEX1.getContainer()
        CUGEX1.set("F1CONO", currentCompany)
        CUGEX1.set("F1FILE", "OOTYPE")
        CUGEX1.set("F1PK01", OOHEAD.get("OAORTP"))
        CUGEX1.set("F1PK02", "")
        CUGEX1.set("F1PK03", "")
        CUGEX1.set("F1PK04", "")
        CUGEX1.set("F1PK05", "")
        CUGEX1.set("F1PK06", "")
        CUGEX1.set("F1PK07", "")
        CUGEX1.set("F1PK08", "")
        if (queryCUGEX1.read(CUGEX1)) {
          chb3 = CUGEX1.get("F1CHB3")
        }
        if (chb3 == 0) {
          mi.error("ORNO - Type de commande " + OOHEAD.get("OAORTP") + " est invalide")
          return
        }
        // Check customer setting
        if (orn2.trim() == "") {
          chb6 = 0
          CUGEX1.set("F1CONO", currentCompany)
          CUGEX1.set("F1FILE", "OCUSMA")
          CUGEX1.set("F1PK01", OOHEAD.get("OACUNO"))
          CUGEX1.set("F1PK02", "")
          CUGEX1.set("F1PK03", "")
          CUGEX1.set("F1PK04", "")
          CUGEX1.set("F1PK05", "")
          CUGEX1.set("F1PK06", "")
          CUGEX1.set("F1PK07", "")
          CUGEX1.set("F1PK08", "")
          if (queryCUGEX1.read(CUGEX1)) {
            chb6 = CUGEX1.get("F1CHB6")
          }
          // The customer wants the charges to be added on an order (orn2) dedicated to the charges. No charges should be added to the original order (orno)
          if (chb6 == 1) {
            return
          }
        }
        ordt = OOHEAD.get("OAORDT") as String
      } else {
        mi.error("ORNO - Numéro de commande " + mi.in.get("ORNO") + " n'existe pas")
        return
      }
    }

    // Check Service charge order
    if(mi.in.get("ORN2") != null && mi.in.get("ORN2") != "") {
      DBAction queryOOHEAD = database.table("OOHEAD").index("00").selection("OACUNO", "OAORST", "OAORTP").build()
      DBContainer OOHEAD = queryOOHEAD.getContainer()
      OOHEAD.set("OACONO", currentCompany)
      OOHEAD.set("OAORNO", mi.in.get("ORN2"))
      if (queryOOHEAD.read(OOHEAD)) {
        orst = OOHEAD.get("OAORST")
        // Check order status
        if (orst.trim() > "69") {
          mi.error("ORN2 - Statut supérieur commande de vente " + orst + " est invalide")
          return
        }
        // Check order type
        chb3 = 0
        DBAction queryCUGEX1 = database.table("CUGEX1").index("00").selection("F1CHB3", "F1CHB6").build()
        DBContainer CUGEX1 = queryCUGEX1.getContainer()
        CUGEX1.set("F1CONO", currentCompany)
        CUGEX1.set("F1FILE", "OOTYPE")
        CUGEX1.set("F1PK01", OOHEAD.get("OAORTP"))
        CUGEX1.set("F1PK02", "")
        CUGEX1.set("F1PK03", "")
        CUGEX1.set("F1PK04", "")
        CUGEX1.set("F1PK05", "")
        CUGEX1.set("F1PK06", "")
        CUGEX1.set("F1PK07", "")
        CUGEX1.set("F1PK08", "")
        if (queryCUGEX1.read(CUGEX1)) {
          chb3 = CUGEX1.get("F1CHB3")
        }
      } else {
        mi.error("ORN2 - Numéro de commande " + mi.in.get("ORN2") + " n'existe pas")
        return
      }
    }
    logger.debug("orno = " + orno)
    logger.debug("ponr = " + ponr)
    logger.debug("orn2 = " + orn2)
    logger.debug("pon2 = " + pon2)

    // Retrieve order line
    cuno = ""
    suno = ""
    hie1 = ""
    hie2 = ""
    hie3 = ""
    hie4 = ""
    hie5 = ""
    a830 = ""
    DBAction queryOOLINE = database.table("OOLINE").index("00").selection("OBCUNO","OBSUNO","OBITNO","OBRORC","OBRORN","OBRORL","OBRORX").build()
    DBContainer OOLINE = queryOOLINE.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", mi.in.get("ORNO")) // Original order
    OOLINE.set("OBPONR", mi.in.get("PONR")) // Original order line
    OOLINE.set("OBPOSX", mi.in.get("POSX")) // Original order line suffix
    if(queryOOLINE.read(OOLINE)){
      cuno = OOLINE.get("OBCUNO")
      suno = OOLINE.get("OBSUNO")
      rorc = OOLINE.get("OBRORC")
      String rorn = OOLINE.get("OBRORN") as String
      int rorl = OOLINE.get("OBRORL") as Integer
      int rorx = OOLINE.get("OBRORX") as Integer
      logger.debug("OOLINE suno = " + suno)

      if(rorc == 2 && rorn != ""){
        logger.debug("OOLINE suno empty, rorc == 2, rorn not empty)")
        DBAction queryMPLINE = database.table("MPLINE").index("00").selection("IBSUNO", "IBWHLO", "IBSUDO", "IBDNDT").build()
        DBContainer MPLINE = queryMPLINE.getContainer()
        MPLINE.set("IBCONO", currentCompany)
        MPLINE.set("IBPUNO", rorn)
        MPLINE.set("IBPNLI", rorl)
        MPLINE.set("IBPNLS", rorx)
        if(queryMPLINE.read(MPLINE)){
          suno = MPLINE.get("IBSUNO").toString()
          String whlo = MPLINE.get("IBWHLO") as String
          String sudo = MPLINE.get("IBSUDO") as String
          int dndt = MPLINE.get("IBDNDT") as Integer
          logger.debug("MPLINE sudo = " + sudo)
          if(sudo != "") {
            DBAction queryPDNHEA = database.table("PDNHEA").index("00").selection("IHCFK5").build()
            DBContainer PDNHEA = queryPDNHEA.getContainer()
            PDNHEA.set("IHCONO", currentCompany)
            PDNHEA.set("IHWHLO", whlo)
            PDNHEA.set("IHSUNO", suno)
            PDNHEA.set("IHSUDO", sudo)
            PDNHEA.set("IHDNDT", dndt)
            if(queryPDNHEA.read(PDNHEA)){
              suno = PDNHEA.get("IHCFK5").toString()
              logger.debug("EXT061 new suno BLI "+ suno +" found")
            }
          }
          logger.debug("MPLINE suno = " + suno)
        }
      }

      // Retrieve item
      DBAction queryMITMAS = database.table("MITMAS").index("00").selection("MMHIE1","MMHIE2","MMHIE3","MMHIE4","MMHIE5").build()
      DBContainer MITMAS = queryMITMAS.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO", OOLINE.get("OBITNO"))
      if(queryMITMAS.read(MITMAS)){
        hie1 = MITMAS.get("MMHIE1")
        hie2 = MITMAS.get("MMHIE2")
        hie3 = MITMAS.get("MMHIE3")
        hie4 = MITMAS.get("MMHIE4")
        hie5 = MITMAS.get("MMHIE5")
      }

      DBAction qCUGEX1 = database.table("CUGEX1").index("00").selection("F1A830").build()
      DBContainer CUGEX1 = qCUGEX1.getContainer()
      CUGEX1.set("F1CONO", currentCompany)
      CUGEX1.set("F1FILE", "MITMAS")
      CUGEX1.set("F1PK01", OOLINE.get("OBITNO"))
      CUGEX1.set("F1PK02", "")
      CUGEX1.set("F1PK03", "")
      CUGEX1.set("F1PK04", "")
      CUGEX1.set("F1PK05", "")
      CUGEX1.set("F1PK06", "")
      CUGEX1.set("F1PK07", "")
      CUGEX1.set("F1PK08", "")
      if (qCUGEX1.read(CUGEX1)) {
        a830 = CUGEX1.get("F1A830")
      }

    } else {
      mi.error("Ligne de commande " + ponr + " n'existe pas")
      return
    }

    logger.debug("cuno = " + cuno)
    logger.debug("suno = " + suno)

    logger.debug("hie5 = " + hie5)
    logger.debug("hie4 = " + hie4)
    logger.debug("hie3 = " + hie3)
    logger.debug("hie2 = " + hie2)
    logger.debug("hie1 = " + hie1)
    logger.debug("a830 = " + a830)

    recordFound = false
    logger.debug("retrieve EXT061 ordt/cuno/suno = " + ordt.trim() + "/" + cuno.trim() + "/" + suno.trim())
    logger.debug("retrieve EXT061 hie1/hie2/hie3/hie4/hie5 = " + hie1.trim() + "/" + hie2.trim() + "/" + hie3.trim() + "/" + hie4.trim() + "/" + hie5.trim() + "/")
    ExpressionFactory expression = database.getExpressionFactory("EXT061")
    expression = expression.le("EXVFDT", ordt)
    expression = expression.and((expression.ge("EXLVDT", ordt)).or(expression.eq("EXLVDT", "0")))
    DBAction queryEXT061 = database.table("EXT061").index("00").matching(expression).selection("EXCRID", "EXCRFA").build()
    DBContainer EXT061 = queryEXT061.getContainer()
    // Read selected charges
    EXT061.set("EXCONO", currentCompany)
    EXT061.set("EXPREX", " 1")
    EXT061.set("EXOBV1", cuno.trim())
    EXT061.set("EXOBV2", a830.trim())
    EXT061.set("EXOBV3", suno.trim())
    EXT061.set("EXOBV4", hie3.trim())
    if (!queryEXT061.readAll(EXT061, 6, nbMaxRecord, outDataEXT061)) {
      logger.debug("EXT061 PREX 1 not found")
      EXT061.set("EXPREX", " 2")
      EXT061.set("EXOBV4", hie2.trim())
      if (!queryEXT061.readAll(EXT061, 6, nbMaxRecord, outDataEXT061)) {
        logger.debug("EXT061 PREX 2 not found")
        EXT061.set("EXPREX", " 3")
        EXT061.set("EXOBV4", hie1.trim())
        if (!queryEXT061.readAll(EXT061, 6, nbMaxRecord, outDataEXT061)) {
          logger.debug("EXT061 PREX 3 not found")
          EXT061.set("EXPREX", " 4")
          EXT061.set("EXOBV4", "")
          if (!queryEXT061.readAll(EXT061, 6, nbMaxRecord, outDataEXT061)) {
            logger.debug("EXT061 PREX 4 not found")
            EXT061.set("EXPREX", " 5")
            EXT061.set("EXOBV3", "")
            if (!queryEXT061.readAll(EXT061, 6, nbMaxRecord, outDataEXT061)) {
              logger.debug("EXT061 PREX 5 not found")
              EXT061.set("EXPREX", " 6")
              EXT061.set("EXOBV2", "")
              if (!queryEXT061.readAll(EXT061, 6, nbMaxRecord, outDataEXT061)) {
                logger.debug("EXT061 PREX 6 not found")
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
  Closure<?> outDataEXT061 = { DBContainer EXT061 ->
    recordFound = true
    crid = EXT061.get("EXCRID")
    crfa = EXT061.get("EXCRFA")
    if(orn2.trim() == "") {
      logger.debug("executeOIS100MIAddLineCharge - Ligne de cde originale ${orno}-${ponr}-${posx}")
      executeOIS100MIAddLineCharge(orno, ponr, posx, crid, crfa)
    } else {
      logger.debug("executeOIS100MIAddLineCharge - Ligne de cde de frais  ${orn2}-${pon2}-${pos2}")
      executeOIS100MIAddLineCharge(orn2, pon2, pos2, crid, crfa)
    }
  }
  // Execute OIS100MI.AddLineCharge
  private executeOIS100MIAddLineCharge(String ORNO, String PONR, String POSX, String CRID, String CRFA){
    Map<String, String> parameters = ["ORNO": ORNO, "PONR": PONR, "POSX": POSX, "CRID": CRID, "CRFA": CRFA]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Erreur OIS100MI AddLineCharge: "+ response.errorMessage)
      } else {
      }
    }
    miCaller.call("OIS100MI", "AddLineCharge", parameters, handler)
  }
}
