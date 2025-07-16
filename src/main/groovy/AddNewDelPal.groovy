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
    currentCompany = (Integer)program.getLDAZD().CONO

    if (mi.in.get("BJNO") == null) {
      jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))
    } else {
      jobNumber = (String)mi.in.get("BJNO")
    }

    //Get mi inputs
    String camu = (mi.in.get("CAMU") != null ? (String)mi.in.get("CAMU") : "")
    long tlix  = (Long)(mi.in.get("TLIX") != null ? mi.in.get("TLIX") : 0)
    String uca4 = (mi.in.get("UCA4") != null ? (String)mi.in.get("UCA4") : "")
    String uca5 = (mi.in.get("UCA5") != null ? (String)mi.in.get("UCA5") : "")
    String uca6 = (mi.in.get("UCA6") != null ? (String)mi.in.get("UCA6") : "")

    logger.debug("EXT050MI.AddNewDelPal bjno:${jobNumber}")


    ExpressionFactory mitaloExpr = database.getExpressionFactory("MITALO")
    mitaloExpr = mitaloExpr.eq("MQCAMU", camu)

    DBAction mitaloQuery = database.table("MITALO").index("10").matching(mitaloExpr).selection("MQRIDN").build()
    DBContainer mitaloRequest = mitaloQuery.getContainer()
    mitaloRequest.set("MQCONO", currentCompany)
    mitaloRequest.set("MQTTYP", 31)


    Closure<?> mitaloReader = { DBContainer mitaloResult ->
    }

    if(!mitaloQuery.readAll(mitaloRequest, 2, nbMaxRecord, mitaloReader)) {
      mi.error("Le numéro de palette " + camu + " n'existe pas")
      return
    }

    if (mi.in.get("TLIX") != null) {
      DBAction queryMhdish = database.table("MHDISH").index("00").selection("OQDLIX").build()
      DBContainer MHDISH = queryMhdish.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU", 1)
      MHDISH.set("OQDLIX", tlix)
      if(!queryMhdish.read(MHDISH)){
        mi.error("Index de livraison  " + tlix + " n'existe pas")
        return
      }
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
        lockedResultEXT059.setInt("EXCHNO", ((Integer)lockedResultEXT059.get("EXCHNO") + 1))
        lockedResultEXT059.set("EXCHID", program.getUser())
        lockedResultEXT059.update()
      }
      queryEXT059.readLock(containerEXT059, updateEXT059)
    } else {
      containerEXT059.set("EXBJNO", jobNumber)
      containerEXT059.set("EXCONO", currentCompany)
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
}
