/**
 * Name: EXT038MI.AddConstraint
 * Migration projet GIT
 * old file = EXT030MI_AddConstraint.groovy
 */

/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT038MI.AddPrtContraint
 * Description : Add records to the EXT038 table.
 * Date         Changed By   Description
 * 20250328     MLECLERCQ      QUAX04 - evol print contrainte
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddPrtContraint extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility


  public AddPrtContraint(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
    this.logger = logger
  }

  public void main() {

    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    boolean isNewBjno = true;

    LocalDateTime timeOfCreation = LocalDateTime.now()

    //Get mi inputs
    String bjno = (mi.in.get("BJNO") != null ? (String) mi.in.get("BJNO") : "")
    String orno = (mi.in.get("ORNO") != null ? (String)mi.in.get("ORNO") : "")
    int ponr = (mi.in.get("PONR") != null ? (Integer)mi.in.get("PONR") : 0)
    int posx = (mi.in.get("POSX") != null ? (Integer)mi.in.get("POSX") : 0)
    String dlix = (mi.in.get("DLIX") != null ? (String)mi.in.get("DLIX") : "")
    String whlo = (mi.in.get("WHLO") != null ? (String)mi.in.get("WHLO") : "")
    String bano = (mi.in.get("BANO") != null ? (String)mi.in.get("BANO") : "")
    String camu = (mi.in.get("CAMU") != null ? (String)mi.in.get("CAMU") : "")
    int zcli = (mi.in.get("ZCLI") != null ? (Integer)mi.in.get("ZCLI") : 0)
    String orst = (mi.in.get("ORST") != null ? (String)mi.in.get("ORST") : "")
    String stat = (mi.in.get("STAT") != null ? (String)mi.in.get("STAT") : "")
    String conn = (mi.in.get("CONN") != null ? (String)mi.in.get("CONN") : "")
    String uca4 = (mi.in.get("UCA4") != null ? (String)mi.in.get("UCA4") : "")
    String cuno = (mi.in.get("CUNO") != null ? (String)mi.in.get("CUNO") : "")
    String itno = (mi.in.get("ITNO") != null ? (String)mi.in.get("ITNO") : "")
    String zagr = (mi.in.get("ZAGR") != null ? (String)mi.in.get("ZAGR") : "")
    String znag = (mi.in.get("ZNAG") != null ? (String)mi.in.get("ZNAG") : "")
    double orqt = (mi.in.get("ORQT") != null ? (Double)mi.in.get("ORQT") : 0)
    int zqco = (mi.in.get("ZQCO") != null ? (Integer)mi.in.get("ZQCO") : 0)
    double ztgr = (mi.in.get("ZTGR") != null ? (Double)mi.in.get("ZTGR") : 0)
    double ztnw = (mi.in.get("ZTNW") != null ? (Double)mi.in.get("ZTNW") : 0)
    int zcid = (mi.in.get("ZCID") != null ? (Integer)mi.in.get("ZCID") : 0)
    String zcod = (mi.in.get("ZCOD") != null ? (String)mi.in.get("ZCOD") : "")
    String zcty = (mi.in.get("ZCTY") != null ? (String)mi.in.get("ZCTY") : "")
    String doid = (mi.in.get("DOID") != null ? (String)mi.in.get("DOID") : "")
    String ads1 = (mi.in.get("ADS1") != null ? (String)mi.in.get("ADS1") : "")


    logger.debug("Received ORNO:${orno}, DLIX:${dlix},CONN:${conn}, BJNO:${bjno},PONR:${ponr},BANO:${bano},ZCID:${zcid},ZCOD:${zcod}")


    //Check if record exists in Constraint Code Table (EXT034)
    if (zcod.length() > 0) {
      DBAction queryEXT034 = database.table("EXT034").index("00").build()
      DBContainer EXT034 = queryEXT034.getContainer()
      EXT034.set("EXCONO", currentCompany)
      EXT034.set("EXZCOD", zcod)
      if (!queryEXT034.read(EXT034)) {
        mi.error("Code contrainte " + zcod + " n'existe pas")
        return
      }
    }

    // check Status
    if(stat == ""){
      stat = "10"
    }
    if (stat != "10" && stat != "20" && stat != "90"){
      mi.error("Statut autorisé : 10, 20 ou 90")
      return
    }

    //Check if CUNO exist in OCUSMA
    if (cuno.length() > 0) {
      DBAction queryOCUSMA = database.table("OCUSMA").index("00").build()
      DBContainer ContainerOCUSMA = queryOCUSMA.getContainer()
      ContainerOCUSMA.set("OKCONO", currentCompany)
      ContainerOCUSMA.set("OKCUNO", cuno)
      if (!queryOCUSMA.read(ContainerOCUSMA)) {
        mi.error("Code client " + cuno + " n'existe pas")
        return
      }
    }
    //Check if WHLO Warehouse in MITWHL
    if (whlo.length() > 0) {
      DBAction queryMITWHL = database.table("MITWHL").index("00").build()
      DBContainer ContainerMITWHL = queryMITWHL.getContainer()
      ContainerMITWHL.set("MWCONO", currentCompany)
      ContainerMITWHL.set("MWWHLO", whlo)
      if (!queryMITWHL.read(ContainerMITWHL)) {
        mi.error("Dépôt " + whlo + " n'existe pas")
        return
      }
    }
    //Check if DLIX exist in MHDISH
    if (dlix.length() > 0 && !dlix.equals("0")) {
      DBAction queryMHDISH = database.table("MHDISH").index("00").build()
      DBContainer ContainerMHDISH = queryMHDISH.getContainer()
      ContainerMHDISH.set("OQCONO", currentCompany)
      ContainerMHDISH.set("OQINOU", 1)
      ContainerMHDISH.set("OQDLIX", Integer.parseInt(dlix))
      if(!queryMHDISH.read(ContainerMHDISH)){
        mi.error("Index de livraison n'existe pas.")
        return
      }
    }

    if (conn.length() > 0 && !conn.equals("0")) {

      DBAction queryMHDISH = database.table("MHDISH").index("20").build()
      DBContainer ContainerMHDISH = queryMHDISH.getContainer()
      ContainerMHDISH.set("OQCONO", currentCompany)
      ContainerMHDISH.set("OQINOU", 1)
      ContainerMHDISH.set("OQCONN", Integer.parseInt(conn))
      if(!queryMHDISH.readAll(ContainerMHDISH,3,{ DBContainer ResultMHDISH ->
      })){
        mi.error("N° de conteneur n'existe pas.")
        return
      }
    }

    if (orno.length() > 0) {
      DBAction queryOOHEAD = database.table("OOHEAD").index("00").build()
      DBContainer ContainerOOHEAD = queryOOHEAD.getContainer()
      ContainerOOHEAD.set("OACONO", currentCompany)
      ContainerOOHEAD.set("OAORNO", orno)
      if(!queryOOHEAD.read(ContainerOOHEAD)){
        mi.error("N° de commande n'existe pas")
        return
      }
    }

    if (itno.length() > 0) {
      DBAction queryMITMAS = database.table("MITMAS").index("00").build()
      DBContainer ContainerMITMAS = queryMITMAS.getContainer()
      ContainerMITMAS.set("MMCONO", currentCompany)
      ContainerMITMAS.set("MMITNO", itno)
      if(!queryMITMAS.read(ContainerMITMAS)){
        mi.error("Article n'existe pas")
        return
      }
    }

    if (bjno.length() > 0) {
      DBAction queryEXT038 = database.table("EXT038").index("00").build()
      DBContainer ContainerEXT038 = queryEXT038.getContainer()
      ContainerEXT038.set("EXCONO", currentCompany)
      ContainerEXT038.set("EXBJNO", bjno as Long)
      if(!queryEXT038.readAll(ContainerEXT038,2,{ DBContainer ResultMHDISH ->
      })){
        mi.error("N° de job n'existe pas")
        return
      }
    }else{
      bjno = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))
      isNewBjno = true;
    }

    logger.debug("DLIX : ${dlix} , CONN:${conn}, ORNO:${orno}")

    DBAction query = database.table("EXT038").index("00").build()
    DBContainer EXT038 = query.getContainer()
    EXT038.set("EXCONO", currentCompany)
    EXT038.set("EXBJNO",bjno as Long)
    EXT038.set("EXORNO", orno)
    EXT038.set("EXPONR", ponr)
    EXT038.set("EXPOSX", posx)
    EXT038.set("EXDLIX", Long.parseLong(dlix))
    EXT038.set("EXWHLO", whlo)
    EXT038.set("EXBANO", bano)
    EXT038.set("EXCAMU", camu)
    EXT038.set("EXZCLI", zcli)
    EXT038.set("EXORST", orst)
    EXT038.set("EXSTAT", stat)
    EXT038.set("EXCONN", Long.parseLong(conn))
    EXT038.set("EXUCA4", uca4)
    EXT038.set("EXCUNO", cuno)
    EXT038.set("EXITNO", itno)
    EXT038.set("EXZAGR", zagr)
    EXT038.set("EXZNAG", znag)
    EXT038.set("EXORQT", orqt)
    EXT038.set("EXZQCO", zqco)
    EXT038.set("EXZTGR", ztgr)
    EXT038.set("EXZTNW", ztnw)
    EXT038.set("EXZCID", zcid)
    EXT038.set("EXZCOD", zcod)
    EXT038.set("EXZCTY", zcty)
    EXT038.set("EXDOID", doid)
    EXT038.set("EXADS1", ads1)
    EXT038.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    EXT038.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
    EXT038.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    EXT038.setInt("EXCHNO", 1)
    EXT038.set("EXCHID", program.getUser())

    if (!query.read(EXT038)) {
      query.insert(EXT038)
      mi.outData.put("BJNO", isNewBjno ? bjno : "")
      mi.write()
    } else {
      mi.error("L'enregistrement existe déjà")
      return
    }
  }
}
