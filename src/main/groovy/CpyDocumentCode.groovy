/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT035MI.CpyDocumentCode
 * Description : Copy records to the EXT035 table.
 * Date         Changed By   Description
 * 20230125     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class CpyDocumentCode extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database

  private int currentCompany

  public CpyDocumentCode(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.logger = logger
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Check if record exists in Constraint Code Table (EXT034)
    if (mi.in.get("ZCOD") != null) {
      DBAction ext034Query = database.table("EXT034").index("00").build()
      DBContainer ext034Request = ext034Query.getContainer()
      ext034Request.set("EXCONO", currentCompany)
      ext034Request.set("EXZCOD", mi.in.get("ZCOD"))
      if (!ext034Query.read(ext034Request)) {
        mi.error("Code contrainte " + mi.in.get("ZCOD") + " n'existe pas")
        return
      }
    }

    //Check if record exists in Constraint Code Table (EXT034)
    if (mi.in.get("CZCO") != null) {
      DBAction ext034Query = database.table("EXT034").index("00").build()
      DBContainer ext034Request = ext034Query.getContainer()
      ext034Request.set("EXCONO", currentCompany)
      ext034Request.set("EXZCOD", mi.in.get("CZCO"))
      if (!ext034Query.read(ext034Request)) {
        mi.error("Code contrainte (to) " + mi.in.get("CZCO") + " n'existe pas")
        return
      }
    }

    //Check if record exists in country Code Table (CSYTAB)
    if (mi.in.get("ZCSC") != null) {
      DBAction csytabQuery = database.table("CSYTAB").index("00").build()
      DBContainer ext034Request = csytabQuery.getContainer()
      ext034Request.set("CTCONO", currentCompany)
      ext034Request.set("CTSTCO", "CSCD")
      ext034Request.set("CTSTKY", mi.in.get("ZCSC"))
      if (!csytabQuery.read(ext034Request)) {
        mi.error("Code pays (to) " + mi.in.get("ZCSC") + " n'existe pas")
        return
      }
    }

    //Check if record Cutomer in Customer Table (OCUSMA)
    if (mi.in.get("ZCUN") != null) {
      DBAction ext034Query = database.table("OCUSMA").index("00").build()
      DBContainer ext034Request = ext034Query.getContainer()
      ext034Request.set("OKCONO", currentCompany)
      ext034Request.set("OKCUNO", mi.in.get("ZCUN"))
      if (!ext034Query.read(ext034Request)) {
        mi.error("Code client (to) " + mi.in.get("ZCUN") + " n'existe pas")
        return
      }
    }

    //Check if record exists in Document Code Table (MPDDOC)
    if (mi.in.get("ZDOI") != null) {
      DBAction ext034Query = database.table("MPDDOC").index("00").selection("DOADS1").build()
      DBContainer ext034Request = ext034Query.getContainer()
      ext034Request.set("DOCONO", currentCompany)
      ext034Request.set("DODOID", mi.in.get("ZDOI"))
      if (!ext034Query.read(ext034Request)) {
        mi.error("Code Document (to) " + mi.in.get("ZDOI") + " n'existe pas")
        return
      }
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext035Query = database.table("EXT035").index("00").selection("EXCSCD", "EXCUNO", "EXDOID", "EXADS1").build()
    DBContainer ext035Request = ext035Query.getContainer()
    ext035Request.set("EXCONO", currentCompany)
    ext035Request.set("EXZCOD", mi.in.get("ZCOD"))
    ext035Request.set("EXCSCD", mi.in.get("CSCD"))
    ext035Request.set("EXCUNO", mi.in.get("CUNO"))
    ext035Request.set("EXDOID", mi.in.get("DOID"))
    if (ext035Query.read(ext035Request)) {
      ext035Request.set("EXZCOD", mi.in.get("CZCO"))
      ext035Request.set("EXCSCD", mi.in.get("ZCSC"))
      ext035Request.set("EXCUNO", mi.in.get("ZCUN"))
      ext035Request.set("EXDOID", mi.in.get("ZDOI"))
      if (!ext035Query.read(ext035Request)) {
        ext035Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext035Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        ext035Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext035Request.setInt("EXCHNO", 1)
        ext035Request.set("EXCHID", program.getUser())
        ext035Query.insert(ext035Request)
      } else {
        mi.error("L'enregistrement existe déjà")
      }
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
