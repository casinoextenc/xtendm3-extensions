/**
 * This extension is used by Mashup
 * Name : EXT020MI.AddAssortCriter
 * Description : The AddAssortCriter transaction adds records to the EXT020 table.
 * description: Add assortment record in EXT010
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddAssortCriter extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany

  public AddAssortCriter(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    int stat = 0
    String cuno = ""
    String ascd = ""
    String fdat = ""

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
      DBAction csytabQuery = database.table("CSYTAB").index("00").build()
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
    stat = (Integer) (mi.in.get("STAT") != null ? mi.in.get("STAT") : 0)

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext020Query = database.table("EXT020").index("00").build()
    DBContainer ext020Request = ext020Query.getContainer()
    ext020Request.set("EXCONO", currentCompany)
    ext020Request.set("EXCUNO", cuno)
    ext020Request.set("EXASCD", ascd)
    ext020Request.setInt("EXFDAT", fdat as Integer)
    if (!ext020Query.read(ext020Request)) {
      ext020Request.setInt("EXSTAT", stat)
      ext020Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext020Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      ext020Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext020Request.setInt("EXCHNO", 1)
      ext020Request.set("EXCHID", program.getUser())
      ext020Query.insert(ext020Request)
    } else {
      mi.error("L'enregistrement existe déjà")
    }
  }
}
