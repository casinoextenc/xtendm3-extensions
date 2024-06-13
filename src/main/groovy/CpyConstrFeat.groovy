/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT033MI.CpyConstrFeat
 * Description : Copy records to the EXT033 table.
 * Date         Changed By   Description
 * 20230125     SEAR         QUAX01 - Constraints matrix 
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class CpyConstrFeat extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database

  private int currentCompany

  public CpyConstrFeat(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.logger = logger
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext033Query = database.table("EXT033").index("00").selection("EXZDES").build()
    DBContainer ext033Request = ext033Query.getContainer()
    ext033Request.set("EXCONO", currentCompany)
    ext033Request.set("EXZCAR", mi.in.get("ZCAR"))
    if(ext033Query.read(ext033Request)){
      ext033Request.set("EXZCAR", mi.in.get("CZCA"))
      if (!ext033Query.read(ext033Request)) {
        ext033Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext033Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        ext033Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext033Request.setInt("EXCHNO", 1)
        ext033Request.set("EXCHID", program.getUser())
        ext033Query.insert(ext033Request)
      } else {
        mi.error("L'enregistrement existe déjà")
      }
    } else {
      mi.error("L'enregistrement n'existe pas")
    }
  }
}
