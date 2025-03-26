/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.UpdShipDelText
 * Description : Update shipment delivery text
 * Date         Changed By   Description
 * 20230526     SEAR         LOG28 - Creation of files and containers
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdShipDelText extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String parm

  private String jobNumber

  public UpdShipDelText(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {

    LocalDateTime timeOfCreation = LocalDateTime.now()

    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Get mi inputs
    long txid  = (Long)(mi.in.get("TXID") != null ? mi.in.get("TXID") : 0)
    String kfld = (mi.in.get("KFLD") != null ? (String)mi.in.get("KFLD") : "")
    String file = (mi.in.get("FILE") != null ? (String)mi.in.get("FILE") : "")

    long kfldFile = kfld as long

    if(!"DCONSI00".equalsIgnoreCase(file.trim()) && !"MHDISH00".equalsIgnoreCase(file.trim())) {
      mi.error("paramètre file doit être DCONSI00 ou MHDISH00")
    }

    if(file.trim() == "DCONSI00") {
      DBAction queryDconsi = database.table("DCONSI").index("00").selection("DACONO", "DACONN", "DATXID").build()
      DBContainer DCONSI = queryDconsi.getContainer()
      DCONSI.set("DACONO", currentCompany)
      DCONSI.set("DACONN", kfldFile)
      if(queryDconsi.read(DCONSI)){
        Closure<?> updateDCONSI = { LockedResult lockedResultDCONSI ->
          lockedResultDCONSI.set("DATXID", txid)
          lockedResultDCONSI.set("DALMDT", utility.call("DateUtil", "currentDateY8AsInt"))
          lockedResultDCONSI.setInt("DACHNO", ((Integer)lockedResultDCONSI.get("DACHNO") + 1))
          lockedResultDCONSI.set("DACHID", program.getUser())
          lockedResultDCONSI.update()
        }
        queryDconsi.readLock(DCONSI, updateDCONSI)
      }
    }

    if(file.trim() == "MHDISH00") {
      DBAction queryExt410 = database.table("EXT410").index("00").selection("EXCONO", "EXDLIX").build()
      DBContainer EXT410 = queryExt410.getContainer()
      EXT410.set("EXCONO", currentCompany)
      EXT410.set("EXDLIX",  kfldFile)
      if(queryExt410.read(EXT410)){
        Closure<?> updateEXT410 = { LockedResult lockedResultEXT410 ->
          lockedResultEXT410.set("EXTXID", txid)
          lockedResultEXT410.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
          lockedResultEXT410.setInt("EXCHNO", ((Integer)lockedResultEXT410.get("EXCHNO") + 1))
          lockedResultEXT410.set("EXCHID", program.getUser())
          lockedResultEXT410.update()
        }
        queryExt410.readLock(EXT410, updateEXT410)
      }
    }
  }
}
