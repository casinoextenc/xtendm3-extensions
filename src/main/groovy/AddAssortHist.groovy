/**
 * This extension is used by Mashup
 * Name : EXT021MI.AddAssortHist
 * COMX01 Gestion des assortiments clients
 * Description : The AddAssortHist transaction adds records to the EXT021 table.
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddAssortHist extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility


  private int currentCompany

  public AddAssortHist(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    String cuno = ""
    String ascd = ""
    String fdat = ""
    String type = ""
    String data = ""
    int chb1 = 0
    String tx60 = ""
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    if (mi.in.get("CUNO") != null) {
      DBAction ocusmaQuery = database.table("OCUSMA").index("00").build()
      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO", currentCompany)
      ocusmaRequest.set("OKCUNO", mi.in.get("CUNO"))
      if (!ocusmaQuery.read(ocusmaRequest)) {
        mi.error("Code Client " + mi.in.get("CUNO") + " n'existe pas")
        return
      }
      cuno = mi.in.get("CUNO")
    } else {
      mi.error("Code Client est obligatoire")
      return
    }
    if (mi.in.get("ASCD") != null) {
      DBAction csytabQuery = database.table("csytab").index("00").build()
      DBContainer csytabRequest = csytabQuery.getContainer()
      csytabRequest.set("CTCONO", currentCompany)
      csytabRequest.set("CTSTCO", "ASCD")
      csytabRequest.set("CTSTKY", mi.in.get("ASCD"))
      if (!csytabQuery.read(csytabRequest)) {
        mi.error("Code Assortiment  " + mi.in.get("ASCD") + " n'existe pas")
        return
      }
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
    if (mi.in.get("TX60") != null) {
      tx60 = mi.in.get("TX60")
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
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
      if (!ext021Query.read(ext021Request)) {
        ext021Request.setInt("EXCHB1", chb1)
        ext021Request.set("EXTX60", tx60)
        ext021Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext021Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        ext021Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext021Request.setInt("EXCHNO", 1)
        ext021Request.set("EXCHID", program.getUser())
        ext021Query.insert(ext021Request)
      } else {
        mi.error("L'enregistrement existe déjà")
        return
      }
    } else {
      mi.error("Entête sélection n'existe pas")
      return
    }
  }
}
