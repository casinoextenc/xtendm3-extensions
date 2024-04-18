/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT031MI.GetConstrType
 * Description : Retrieve records from the EXT031 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class GetConstrType extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  public GetConstrType(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
    this.mi = mi
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
    DBAction query = database.table("EXT031").index("00").selection("EXZCTY", "EXZTYP", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer EXT031 = query.getContainer()
    EXT031.set("EXCONO", currentCompany)
    EXT031.set("EXZCTY",  mi.in.get("ZCTY"))
    if(!query.readAll(EXT031, 2, outData)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  Closure<?> outData = { DBContainer EXT031 ->
    String consraintType = EXT031.get("EXZCTY")
    String description = EXT031.get("EXZTYP")
    String entryDate = EXT031.get("EXRGDT")
    String entryTime = EXT031.get("EXRGTM")
    String changeDate = EXT031.get("EXLMDT")
    String changeNumber = EXT031.get("EXCHNO")
    String changedBy = EXT031.get("EXCHID")
    mi.outData.put("ZCTY", consraintType)
    mi.outData.put("ZTYP", description)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }
}

