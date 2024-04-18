/**                    
* Name: EXT050MI.AddNewDelPal
* Migration projet GIT
* old file = EXT050MI_AddNewDelPal.groovy
*/


/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.AddNewDelPal
 * Description : batch template
 * Date         Changed By   Description
 * 20230526     SEAR         LOG28 - Creation of files and containers
 */
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

  public AddNewDelPal(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
    this.logger = logger
  }

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


    ExpressionFactory MITALO_expr = database.getExpressionFactory("MITALO")
    MITALO_expr = MITALO_expr.eq("MQCAMU", camu)
    
    DBAction MITALO_query = database.table("MITALO").index("10").matching(MITALO_expr).selection("MQRIDN").build()
    DBContainer MITALO_request = MITALO_query.getContainer()
    MITALO_request.set("MQCONO", currentCompany)
    MITALO_request.set("MQTTYP", 31)
    
    
    Closure<?> MITALO_reader = { DBContainer MITALO_result ->
    }
    
    if(!MITALO_query.readAll(MITALO_request, 2, MITALO_reader)) {
      mi.error("Le numéro de palette " + camu + " n'existe pas")
      return
    }

    if (mi.in.get("TLIX") != null) {
      DBAction query_MHDISH = database.table("MHDISH").index("00").selection("OQDLIX").build()
      DBContainer MHDISH = query_MHDISH.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU", 1)
      MHDISH.set("OQDLIX", tlix)
      if(!query_MHDISH.read(MHDISH)){
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
