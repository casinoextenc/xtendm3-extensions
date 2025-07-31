/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.ChgOrderHead
 * Description : Change order head
 * Date         Changed By   Version      Description
 * 20230413     RENARN        1.0         LOG28 - Creation of files and containers
 * 20250425     RENARN        1.1         LOG28 - Added executeOIS100MIUpdUserDefCOH
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

    executeOIS100MIUpdUserDefCOH(orno, uca4, uca5, uca6)
  }
  /**
   * This method is used to call the MI OIS100MI UpdUserDefCOH
   * @param ORNO Order number
   * @param UCA4 User defined field 4
   * @param UCA5 User defined field 5
   * @param UCA6 User defined field 6
   */
  private executeOIS100MIUpdUserDefCOH(String ORNO, String UCA4, String UCA5, String UCA6){
    Map<String, String> parameters = ["ORNO": ORNO, "UCA4": UCA4, "UCA5": UCA5, "UCA6": UCA6]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        mi.error(response.errorMessage)
      } else {
      }
    }
    miCaller.call("OIS100MI", "UpdUserDefCOH", parameters, handler)
  }
}
