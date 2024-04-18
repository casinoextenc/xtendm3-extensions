/**
 * README
 * This extension is used by extensions
 *
 * Name : EXT800MI.LstParam
 * Description : List general settings by extension
 * Date         Changed By   Description
 * 20210817     RENARN       APPX25 - Management of transport costs
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class LstParam extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  public LstParam(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
    this.mi = mi;
    this.database = database
    this.logger = logger
    this.program = program
  }

  public void main() {
    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    if (mi.in.get("EXNM") == null) {
      DBAction query = database.table("EXT800").index("00").selection("EXEXNM", "EXP001", "EXP002", "EXP003", "EXP004", "EXP005", "EXP006", "EXP007", "EXP008", "EXP009", "EXP010", "EXP011", "EXP012", "EXP013", "EXP014", "EXP015", "EXP016", "EXP017", "EXP018", "EXP019", "EXP020", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer EXT800 = query.getContainer()
      EXT800.set("EXCONO", currentCompany)
      if(!query.readAll(EXT800, 1, outData)){
        mi.error("L'enregistrement n'existe pas")
        return
      }
    } else {
      String constraintType = mi.in.get("EXNM")
      ExpressionFactory expression = database.getExpressionFactory("EXT800")
      expression = expression.ge("EXEXNM", constraintType)
      DBAction query = database.table("EXT800").index("00").matching(expression).selection("EXEXNM", "EXP001", "EXP002", "EXP003", "EXP004", "EXP005", "EXP006", "EXP007", "EXP008", "EXP009", "EXP010", "EXP011", "EXP012", "EXP013", "EXP014", "EXP015", "EXP016", "EXP017", "EXP018", "EXP019", "EXP020", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
      DBContainer EXT800 = query.getContainer()
      EXT800.set("EXCONO", currentCompany)
      if(!query.readAll(EXT800, 1, outData)){
        mi.error("L'enregistrement n'existe pas")
        return
      }
    }
  }

  Closure<?> outData = { DBContainer EXT800 ->
    String exnm = EXT800.get("EXEXNM")
    String Param1 = EXT800.get("EXP001")
    String Param2 = EXT800.get("EXP002")
    String Param3 = EXT800.get("EXP003")
    String Param4 = EXT800.get("EXP004")
    String Param5 = EXT800.get("EXP005")
    String Param6 = EXT800.get("EXP006")
    String Param7 = EXT800.get("EXP007")
    String Param8 = EXT800.get("EXP008")
    String Param9 = EXT800.get("EXP009")
    String Param10 = EXT800.get("EXP010")
    String Param11 = EXT800.get("EXP011")
    String Param12 = EXT800.get("EXP012")
    String Param13 = EXT800.get("EXP013")
    String Param14 = EXT800.get("EXP014")
    String Param15 = EXT800.get("EXP015")
    String Param16 = EXT800.get("EXP016")
    String Param17 = EXT800.get("EXP017")
    String Param18 = EXT800.get("EXP018")
    String Param19 = EXT800.get("EXP019")
    String Param20 = EXT800.get("EXP020")
    String entryDate = EXT800.get("EXRGDT")
    String entryTime = EXT800.get("EXRGTM")
    String changeDate = EXT800.get("EXLMDT")
    String changeNumber = EXT800.get("EXCHNO")
    String changedBy = EXT800.get("EXCHID")
    mi.outData.put("EXNM", exnm)
    mi.outData.put("P001", Param1)
    mi.outData.put("P002", Param2)
    mi.outData.put("P003", Param3)
    mi.outData.put("P004", Param4)
    mi.outData.put("P005", Param5)
    mi.outData.put("P006", Param6)
    mi.outData.put("P007", Param7)
    mi.outData.put("P008", Param8)
    mi.outData.put("P009", Param9)
    mi.outData.put("P010", Param10)
    mi.outData.put("P011", Param11)
    mi.outData.put("P012", Param12)
    mi.outData.put("P013", Param13)
    mi.outData.put("P014", Param14)
    mi.outData.put("P015", Param15)
    mi.outData.put("P016", Param16)
    mi.outData.put("P017", Param17)
    mi.outData.put("P018", Param18)
    mi.outData.put("P019", Param19)
    mi.outData.put("P020", Param20)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }
}
