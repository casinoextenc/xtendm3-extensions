/**
 * This extension is used by Mashup
 * Name : EXT021MI.UpdAssortHist
 * COMX01 Gestion des assortiments clients
 * Description : The UpdAssortHist transaction update records to the EXT021 table.
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdAssortHist extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  public UpdAssortHist(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    Integer currentCompany
    String cuno = ""
    String ascd = ""
    String fdat = ""
    String type = ""
    String data = ""
    int chb1 = 0
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    if (mi.in.get("CUNO") != null) {
      cuno = mi.in.get("CUNO")
    } else {
      mi.error("Code Client est obligatoire")
      return
    }
    if (mi.in.get("ASCD") != null) {
      ascd = mi.in.get("ASCD")
    } else {
      mi.error("Code Assortiment est obligatoire")
      return
    }
    if (mi.in.get("FDAT") == null) {
      mi.error("Date de Validité est obligatoire")
      return
    } else {
      fdat = mi.in.get("FDAT")
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        mi.error("Format Date de Validité incorrect")
        return
      }
    }
    if (mi.in.get("TYPE") == null) {
      mi.error("Le type est obligatoire")
      return
    } else {
      type = mi.in.get("TYPE")
    }

    if (mi.in.get("CHB1") == null) {
      chb1 = 0
    } else {
      if (mi.in.get("CHB1") == 1) {
        chb1 = 1
      } else {
        chb1 = 0
      }
    }
    if (mi.in.get("DATA") != null) {
      data = mi.in.get("DATA")
    }

    DBAction ext020Query = database.table("EXT020").index("00").build()
    DBContainer ext020Request = ext020Query.getContainer()
    ext020Request.set("EXCONO", currentCompany)
    ext020Request.set("EXCUNO", cuno)
    ext020Request.set("EXASCD", ascd)
    ext020Request.setInt("EXFDAT", fdat as Integer)
    if (ext020Query.read(ext020Request)) {
      DBAction ext021Query = database.table("EXT021").index("00").build()
      DBContainer ext021Request = ext021Query.getContainer()
      ext021Request.set("EXCONO", currentCompany)
      ext021Request.set("EXCUNO", cuno)
      ext021Request.set("EXASCD", ascd)
      ext021Request.setInt("EXFDAT", fdat as Integer)
      ext021Request.set("EXTYPE", type)
      ext021Request.set("EXDATA", data)

      if (!ext021Query.readLock(ext021Request, ext021Udpater)) {
        mi.error("L'enregistrement n'existe pas")
        return
      }
    } else {
      mi.error("Le Critères assortiment existe déjà")
      return
    }
  }
  Closure<?> ext021Udpater = {
    LockedResult ext021LockedResult ->
      LocalDateTime timeOfCreation = LocalDateTime.now()
      int changeNumber = ext021LockedResult.get("EXCHNO")
      if (mi.in.get("CHB1") != null) {
        ext021LockedResult.set("EXCHB1", mi.in.get("CHB1"))
      }
      if (mi.in.get("TX60") != null) {
        ext021LockedResult.set("EXTX60", mi.in.get("TX60"))
      }
      ext021LockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext021LockedResult.setInt("EXCHNO", changeNumber + 1)
      ext021LockedResult.set("EXCHID", program.getUser())
      ext021LockedResult.update()
  }
}
