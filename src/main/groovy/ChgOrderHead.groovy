/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.ChgOrderHead
 * Description : Change order head
 * Date         Changed By   Description
 * 20230413     RENARN       LOG28 - Creation of files and containers
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class ChgOrderHead extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String parm

  public ChgOrderHead(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {

    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Get mi inputs
    String orno = (mi.in.get("ORNO") != null ? (String)mi.in.get("ORNO") : "")
    String uca4 = (mi.in.get("UCA4") != null ? (String)mi.in.get("UCA4") : "")
    String uca5 = (mi.in.get("UCA5") != null ? (String)mi.in.get("UCA5") : "")
    String uca6 = (mi.in.get("UCA6") != null ? (String)mi.in.get("UCA6") : "")
    DBAction OOHEAD_query = database.table("OOHEAD").index("00").selection("OAORNO").build()
    DBContainer OOHEAD = OOHEAD_query.getContainer()
    OOHEAD.set("OACONO", currentCompany)
    OOHEAD.set("OAORNO", orno)

    // Update OOHEAD
    Closure<?> updateCallBack = { LockedResult lockedResult ->
      LocalDateTime timeOfCreation = LocalDateTime.now()
      int changeNumber = lockedResult.get("OACHNO")
      if (mi.in.get("UCA4") != null)
        lockedResult.set("OAUCA4", uca4)
      if (mi.in.get("UCA5") != null)
        lockedResult.set("OAUCA5", uca5)
      if (mi.in.get("UCA6") != null)
        lockedResult.set("OAUCA6", uca6)
      lockedResult.setInt("OALMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      lockedResult.setInt("OACHNO", changeNumber + 1)
      lockedResult.set("OACHID", program.getUser())
      lockedResult.update()
    }

    if(!OOHEAD_query.readLock(OOHEAD, updateCallBack)) {
      mi.error("Le numéro de commande " + orno + " n'existe pas")
    }
  }
}