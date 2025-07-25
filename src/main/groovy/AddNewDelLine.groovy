/****************************************************************************************
 Extension Name: EXT050MI.AddNewDelLine
 Type: ExtendM3Transaction
 Script Author: SEAR
 Date: 2023-05-26
 Description:
 * Add new delivery line

 Revision History:
 Name        Date        Version   Description of Changes
 SEAR        2023-05-26  1.0       LOG28 - Creation of files and containers
 ARENARD     2025-04-22  1.1       Code has been checked
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddNewDelLine extends ExtendM3Transaction {
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

  public AddNewDelLine(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
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
    String orno = (mi.in.get("ORNO") != null ? (String)mi.in.get("ORNO") : "")
    int ponr = (int)(mi.in.get("PONR") != null ? mi.in.get("PONR") : 0)
    int posx = (int)(mi.in.get("POSX") != null ? mi.in.get("POSX") : 0)
    double alqt = (double) (mi.in.get("ALQT") != null ? mi.in.get("ALQT") : 0)
    long tlix  = (Long)(mi.in.get("TLIX") != null ? mi.in.get("TLIX") : 0)
    String dlix = null

    DBAction ooheadQuery = database.table("OOHEAD").index("00").selection("OAORNO").build()
    DBContainer OOHEAD = ooheadQuery.getContainer()
    OOHEAD.set("OACONO", currentCompany)
    OOHEAD.set("OAORNO", orno)
    if(!ooheadQuery.read(OOHEAD)) {
      mi.error("Le numéro de commande " + orno + " n'existe pas")
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


    DBAction mhdislQuery = database.table("MHDISL").index("10").build()
    DBContainer mhdislRequest = mhdislQuery.getContainer()
    mhdislRequest.set("URCONO", currentCompany)
    mhdislRequest.set("URRORC", 3)
    mhdislRequest.set("URRIDN", orno)
    mhdislRequest.set("URRIDL", ponr)
    mhdislRequest.set("URRIDX", posx)

    Closure<?> closureMhdisl = { DBContainer mhdislResult ->
      if (dlix == null)
        dlix = mhdislResult.get("URDLIX") as String
    }



    if (!mhdislQuery.readAll(mhdislRequest, 5, nbMaxRecord, closureMhdisl)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }



    //Check if record exists
    DBAction queryEXT057 = database.table("EXT057")
      .index("00")
      .selection(
        "EXCONO",
        "EXBJNO",
        "EXORNO",
        "EXPONR",
        "EXPOSX",
        "EXALQT",
        "EXTLIX",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer containerEXT057 = queryEXT057.getContainer()
    containerEXT057.set("EXBJNO", jobNumber)
    containerEXT057.set("EXCONO", currentCompany)
    containerEXT057.set("EXORNO", orno)
    containerEXT057.set("EXPONR", ponr)
    containerEXT057.set("EXPOSX", posx)
    containerEXT057.set("EXALQT", alqt)
    containerEXT057.set("EXTLIX", tlix)

    //Record exists
    if (queryEXT057.read(containerEXT057)) {
      Closure<?> updateEXT057 = { LockedResult lockedResultEXT057 ->
        lockedResultEXT057.set("EXBJNO", jobNumber)
        lockedResultEXT057.set("EXCONO", currentCompany)
        lockedResultEXT057.set("EXORNO", orno)
        lockedResultEXT057.set("EXPONR", ponr)
        lockedResultEXT057.set("EXPOSX", posx)
        lockedResultEXT057.set("EXALQT", alqt)
        lockedResultEXT057.set("EXTLIX", tlix)
        lockedResultEXT057.set("EXDLIX", dlix as long)
        lockedResultEXT057.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        lockedResultEXT057.setInt("EXCHNO", ((Integer)lockedResultEXT057.get("EXCHNO") + 1))
        lockedResultEXT057.set("EXCHID", program.getUser())
        lockedResultEXT057.set("EXALUN", 'UPA')
        lockedResultEXT057.update()
      }
      queryEXT057.readLock(containerEXT057, updateEXT057)
    } else {
      containerEXT057.set("EXBJNO", jobNumber)
      containerEXT057.set("EXCONO", currentCompany)
      containerEXT057.set("EXORNO", orno)
      containerEXT057.set("EXPONR", ponr)
      containerEXT057.set("EXPOSX", posx)
      containerEXT057.set("EXALQT", alqt)
      containerEXT057.set("EXTLIX", tlix)
      containerEXT057.set("EXDLIX", dlix as long)
      containerEXT057.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerEXT057.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
      containerEXT057.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerEXT057.set("EXCHNO", 1)
      containerEXT057.set("EXCHID", program.getUser())
      containerEXT057.set("EXALUN", 'UPA')
      queryEXT057.insert(containerEXT057)
    }

    mi.outData.put("BJNO", jobNumber)
    mi.write()
  }
}
