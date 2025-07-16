/****************************************************************************************
 * Extension Name : EXT038MI.AddPrtContraint
 * Type: ExtendM3Transaction :
 * Description :  Add print contraint to the EXT038 table
 * Script Author: Maxime MLECLERCQ
 * Date : 20250328
 *
 * Revision History:
 * Name        Date        Version  Description of Changes
 * MLECLERCQ  20250328    1.0.0    QUAX04 - evol print contrainte
 * FLEBARS    20250626    1.0.1    QUAX04 - Code reiview for validation
 *
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddPrtContraint extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
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
      DBAction ext034Query = database.table("EXT034").index("00").build()
      DBContainer ext034Request = ext034Query.getContainer()
      ext034Request.set("EXCONO", currentCompany)
      ext034Request.set("EXZCOD", zcod)
      if (!ext034Query.read(ext034Request)) {
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
      DBAction ocusmaQuery = database.table("OCUSMA").index("00").build()
      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO", currentCompany)
      ocusmaRequest.set("OKCUNO", cuno)
      if (!ocusmaQuery.read(ocusmaRequest)) {
        mi.error("Code client " + cuno + " n'existe pas")
        return
      }
    }
    //Check if WHLO Warehouse in MITWHL
    if (whlo.length() > 0) {
      DBAction mitwhlQuery = database.table("MITWHL").index("00").build()
      DBContainer mitwhlRequest = mitwhlQuery.getContainer()
      mitwhlRequest.set("MWCONO", currentCompany)
      mitwhlRequest.set("MWWHLO", whlo)
      if (!mitwhlQuery.read(mitwhlRequest)) {
        mi.error("Dépôt " + whlo + " n'existe pas")
        return
      }
    }
    //Check if DLIX exist in MHDISH
    if (dlix.length() > 0 && !dlix.equals("0")) {
      DBAction mhdishQuery = database.table("MHDISH").index("00").build()
      DBContainer mhdishRequest = mhdishQuery.getContainer()
      mhdishRequest.set("OQCONO", currentCompany)
      mhdishRequest.set("OQINOU", 1)
      mhdishRequest.set("OQDLIX", Long.parseLong(dlix))
      if(!mhdishQuery.read(mhdishRequest)){
        mi.error("Index de livraison n'existe pas.")
        return
      }
    }

    if (conn.length() > 0 && !conn.equals("0")) {

      DBAction mhdishQuery = database.table("MHDISH").index("20").build()
      DBContainer ùhdishRequest = mhdishQuery.getContainer()
      ùhdishRequest.set("OQCONO", currentCompany)
      ùhdishRequest.set("OQINOU", 1)
      ùhdishRequest.set("OQCONN", Integer.parseInt(conn))
      if(!mhdishQuery.readAll(ùhdishRequest,3,{ DBContainer ResultMHDISH ->
      })){
        mi.error("N° de conteneur n'existe pas.")
        return
      }
    }

    if (orno.length() > 0) {
      DBAction ooheadQuery = database.table("OOHEAD").index("00").build()
      DBContainer ooheadRequest = ooheadQuery.getContainer()
      ooheadRequest.set("OACONO", currentCompany)
      ooheadRequest.set("OAORNO", orno)
      if(!ooheadQuery.read(ooheadRequest)){
        mi.error("N° de commande n'existe pas")
        return
      }
    }

    if (itno.length() > 0) {
      DBAction mitmasQuery = database.table("MITMAS").index("00").build()
      DBContainer mitmasRequest = mitmasQuery.getContainer()
      mitmasRequest.set("MMCONO", currentCompany)
      mitmasRequest.set("MMITNO", itno)
      if(!mitmasQuery.read(mitmasRequest)){
        mi.error("Article n'existe pas")
        return
      }
    }

    if (bjno.length() > 0) {
      DBAction ext038Query = database.table("EXT038").index("00").build()
      DBContainer ext038Request = ext038Query.getContainer()
      ext038Request.set("EXCONO", currentCompany)
      ext038Request.set("EXBJNO", bjno as Long)
      if(!ext038Query.readAll(ext038Request,2,10000, { DBContainer resultMHDISH ->
      })){
        mi.error("N° de job n'existe pas")
        return
      }
    }else{
      bjno = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))
      isNewBjno = true;
    }

    logger.debug("DLIX : ${dlix} , CONN:${conn}, ORNO:${orno}")

    DBAction ext038Query = database.table("EXT038").index("00").build()
    DBContainer ext038Request = ext038Query.getContainer()
    ext038Request.set("EXCONO", currentCompany)
    ext038Request.set("EXBJNO",bjno as Long)
    ext038Request.set("EXORNO", orno)
    ext038Request.set("EXPONR", ponr)
    ext038Request.set("EXPOSX", posx)
    ext038Request.set("EXDLIX", Long.parseLong(dlix))
    ext038Request.set("EXWHLO", whlo)
    ext038Request.set("EXBANO", bano)
    ext038Request.set("EXCAMU", camu)
    ext038Request.set("EXZCLI", zcli)
    ext038Request.set("EXORST", orst)
    ext038Request.set("EXSTAT", stat)
    ext038Request.set("EXCONN", Long.parseLong(conn))
    ext038Request.set("EXUCA4", uca4)
    ext038Request.set("EXCUNO", cuno)
    ext038Request.set("EXITNO", itno)
    ext038Request.set("EXZAGR", zagr)
    ext038Request.set("EXZNAG", znag)
    ext038Request.set("EXORQT", orqt)
    ext038Request.set("EXZQCO", zqco)
    ext038Request.set("EXZTGR", ztgr)
    ext038Request.set("EXZTNW", ztnw)
    ext038Request.set("EXZCID", zcid)
    ext038Request.set("EXZCOD", zcod)
    ext038Request.set("EXZCTY", zcty)
    ext038Request.set("EXDOID", doid)
    ext038Request.set("EXADS1", ads1)
    ext038Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    ext038Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
    ext038Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    ext038Request.setInt("EXCHNO", 1)
    ext038Request.set("EXCHID", program.getUser())

    if (!ext038Query.read(ext038Request)) {
      ext038Query.insert(ext038Request)
      mi.outData.put("BJNO", isNewBjno ? bjno : "")
      mi.write()
    } else {
      mi.error("L'enregistrement existe déjà")
      return
    }
  }
}
