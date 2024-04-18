/**
* README
* This extension is used by Mashup
* 
* Name : EXT034MI.GetCodification
* Description : Retrieve records from the EXT034 table.
* Date         Changed By   Description
* 20210125     SEAR         QUAX01 - Constraints matrix 
* 20230620     FLEBARS      QUAX01 - evol contrainte 
*/
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class GetCodification extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  public GetCodification(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
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
    DBAction query = database.table("EXT034").index("00").selection(
      "EXZCOD"
      , "EXZDES"
      , "EXZCTY"
      , "EXRGDT"
      , "EXRGTM"
      , "EXLMDT"
      , "EXCHNO"
      , "EXZSTY"
      , "EXCHID").build()
    DBContainer EXT034 = query.getContainer()
    EXT034.set("EXCONO", currentCompany)
    EXT034.set("EXZCOD",  mi.in.get("ZCOD"))
    if(!query.readAll(EXT034, 2, outData)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  Closure<?> outData = { DBContainer EXT034 ->
    String codification = EXT034.get("EXZCOD")
    String description = EXT034.get("EXZDES")
    String constraintType = EXT034.get("EXZCTY")
    String entryDate = EXT034.get("EXRGDT")
    String entryTime = EXT034.get("EXRGTM")
    String changeDate = EXT034.get("EXLMDT")
    String changeNumber = EXT034.get("EXCHNO")
    String changedBy = EXT034.get("EXCHID")
    String zsty = EXT034.get("EXZSTY")
    mi.outData.put("ZCOD", codification)
    mi.outData.put("ZCTY", constraintType)
    mi.outData.put("ZDES", description)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.outData.put("ZSTY", zsty)
    mi.write()
  }
}

