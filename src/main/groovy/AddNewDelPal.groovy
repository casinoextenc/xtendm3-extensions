/****************************************************************************************
 Extension Name: EXT050MI.AddNewDelPal
 Type: ExtendM3Transaction
 Script Author: SEAR
 Date: 2023-05-26
 Description:
 * Add new delivery pallet

 Revision History:
 Name        Date        Version   Description of Changes
 SEAR        2023-05-26  1.0       LOG28 - Creation of files and containers
 ARENARD     2025-04-22  1.1       Code has been checked
 FLEBARS     2025-12-04  1.2       Fix issues
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddNewDelPal extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String parm
  private int currentCompany

  private String jobNumber
  private Integer nbMaxRecord = 10000

  public AddNewDelPal(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
    this.logger = logger
  }

  // Main
  public void main() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentCompany = (Integer) program.getLDAZD().CONO

    if (mi.in.get("BJNO") == null) {
      jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))
    } else {
      jobNumber = (String) mi.in.get("BJNO")
    }

    //Get mi inputs
    String camu = (mi.in.get("CAMU") != null ? (String) mi.in.get("CAMU") : "")
    String orno = (mi.in.get("ORNO") != null ? (String) mi.in.get("ORNO") : "")
    long tlix = (Long) (mi.in.get("TLIX") != null ? mi.in.get("TLIX") : 0)
    String uca4 = (mi.in.get("UCA4") != null ? (String) mi.in.get("UCA4") : "")
    String uca5 = (mi.in.get("UCA5") != null ? (String) mi.in.get("UCA5") : "")
    String uca6 = (mi.in.get("UCA6") != null ? (String) mi.in.get("UCA6") : "")

    logger.debug("EXT050MI.AddNewDelPal bjno:${jobNumber}")

    if (mi.in.get("TLIX") != null) {
      DBAction queryMhdish = database.table("MHDISH").index("00").selection("OQDLIX").build()
      DBContainer MHDISH = queryMhdish.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU", 1)
      MHDISH.set("OQDLIX", tlix)
      if (!queryMhdish.read(MHDISH)) {
        mi.error("Index de livraison  " + tlix + " n'existe pas")
        return
      }
    }


    ExpressionFactory mitaloExpr = database.getExpressionFactory("MITALO")
    mitaloExpr = mitaloExpr.eq("MQCAMU", camu)
    mitaloExpr = mitaloExpr.eq("MQRIDN", orno)

    DBAction mitaloQuery = database.table("MITALO").index("10").matching(mitaloExpr).selection("MQRIDN").build()
    DBContainer mitaloRequest = mitaloQuery.getContainer()
    mitaloRequest.set("MQCONO", currentCompany)
    mitaloRequest.set("MQTTYP", 31)

    if (!checkOrder(orno, uca4, uca5, uca6)) {
      mi.error("La commande ${orno} ne correspond pas au dossier selectionné ${uca4} ${uca5} ${uca6}")
      return
    }
    Closure<?> mitaloReader = { DBContainer mitaloResult ->
    }

    if (!mitaloQuery.readAll(mitaloRequest, 2, nbMaxRecord, mitaloReader)) {
      mi.error("Le numéro de palette " + camu + " n'existe pas")
      return
    }

    //Check if record exists
    DBAction queryEXT059 = database.table("EXT059")
      .index("00")
      .selection(
        "EXCONO",
        "EXBJNO",
        "EXORNO",
        "EXCAMU",
        "EXTLIX",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer containerEXT059 = queryEXT059.getContainer()
    containerEXT059.set("EXBJNO", jobNumber)
    containerEXT059.set("EXCONO", currentCompany)
    containerEXT059.set("EXCAMU", camu)
    containerEXT059.set("EXTLIX", tlix)

    //Record exists
    if (queryEXT059.read(containerEXT059)) {
      Closure<?> updateEXT059 = { LockedResult lockedResultEXT059 ->
        lockedResultEXT059.set("EXBJNO", jobNumber)
        lockedResultEXT059.set("EXCONO", currentCompany)
        lockedResultEXT059.set("EXCAMU", camu)
        lockedResultEXT059.set("EXTLIX", tlix)
        lockedResultEXT059.set("EXUCA4", uca4)
        lockedResultEXT059.set("EXUCA5", uca5)
        lockedResultEXT059.set("EXUCA6", uca6)
        lockedResultEXT059.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        lockedResultEXT059.setInt("EXCHNO", ((Integer) lockedResultEXT059.get("EXCHNO") + 1))
        lockedResultEXT059.set("EXCHID", program.getUser())
        lockedResultEXT059.update()
      }
      queryEXT059.readLock(containerEXT059, updateEXT059)
    } else {
      containerEXT059.set("EXBJNO", jobNumber)
      containerEXT059.set("EXCONO", currentCompany)
      containerEXT059.set("EXORNO", orno)
      containerEXT059.set("EXCAMU", camu)
      containerEXT059.set("EXTLIX", tlix)
      containerEXT059.set("EXUCA4", uca4)
      containerEXT059.set("EXUCA5", uca5)
      containerEXT059.set("EXUCA6", uca6)
      containerEXT059.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerEXT059.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
      containerEXT059.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerEXT059.set("EXCHNO", 1)
      containerEXT059.set("EXCHID", program.getUser())
      queryEXT059.insert(containerEXT059)
    }
    mi.outData.put("BJNO", jobNumber)
    mi.write()
  }

  /**
   * Control if order match with uca4, uca5, uca6
   * @param orno
   * @param uca4
   * @param uca5
   * @param uca6
   * @return
   */
  private boolean checkOrder(String orno, String uca4, String uca5, String uca6) {
    DBAction ooheadQuery = database.table("OOHEAD")
      .index("00")
      .selection("OAUCA4"
        , "OAUCA5"
        , "OAUCA6"
      )
      .build()

    DBContainer ooheadRequest = ooheadQuery.getContainer()
    ooheadRequest.set("OACONO", currentCompany)
    ooheadRequest.set("OAORNO", orno)
    if (ooheadQuery.read(ooheadRequest)) {
      String tUca4 = ooheadRequest.getString("OAUCA4").trim()
      String tUca5 = ooheadRequest.getString("OAUCA5").trim()
      String tUca6 = ooheadRequest.getString("OAUCA6").trim()
      logger.debug("checkorno ${tUca4} ${tUca5} ${tUca6}" + (tUca4.equals(uca4) && tUca5.equals(uca5) && tUca6.equals(uca6)))
      if (tUca4.equals(uca4) && tUca5.equals(uca5) && tUca6.equals(uca6)) {
        return true
      }
    }
    return false


  }

}
