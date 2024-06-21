/**
 * This extension is used by Mashup
 * Name : EXT020MI.GetAssortCriter
 * COMX01 Gestion des assortiments clients
 * Description : The GetAssortCriter transaction get records to the EXT020 table.
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */
import java.time.LocalDateTime

public class GetAssortCriter extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  public GetAssortCriter(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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
      mi.error("Code Assortiment  " + mi.in.get("ASCD") + " n'existe pas")
      return
    }

    if (mi.in.get("FDAT") != null) {
      fdat = mi.in.get("FDAT")
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        mi.error("Format Date de Validité incorrect")
        return
      }
    } else {
      mi.error("Date de Validité est obligatoire")
      return
    }
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext032Query = database.table("EXT020").index("00").selection("EXCONO", "EXASCD", "EXCUNO", "EXFDAT", "EXSTAT", "EXSTTS", "EXNDTS", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer ext032Request = ext032Query.getContainer()
    ext032Request.set("EXCONO", currentCompany)
    ext032Request.set("EXCUNO", cuno)
    ext032Request.set("EXASCD", ascd)
    ext032Request.setInt("EXFDAT", fdat as Integer)
    if (!ext032Query.readAll(ext032Request, 4, 10000, ext020Reader)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  Closure<?> ext020Reader = { DBContainer ext020Resut ->
    String cono = ext020Resut.get("EXCONO")
    String cuno = ext020Resut.get("EXCUNO")
    String ascd = ext020Resut.get("EXASCD")
    String fdat = ext020Resut.get("EXFDAT")
    String stat = ext020Resut.get("EXSTAT")
    String stts = ext020Resut.get("EXSTTS")
    String ndts = ext020Resut.get("EXNDTS")
    String entryDate = ext020Resut.get("EXRGDT")
    String entryTime = ext020Resut.get("EXRGTM")
    String changeDate = ext020Resut.get("EXLMDT")
    String changeNumber = ext020Resut.get("EXCHNO")
    String changedBy = ext020Resut.get("EXCHID")

    mi.outData.put("CONO", cono)
    mi.outData.put("CUNO", cuno)
    mi.outData.put("ASCD", ascd)
    mi.outData.put("FDAT", fdat)
    mi.outData.put("STAT", stat)
    mi.outData.put("STTS", stts)
    mi.outData.put("NDTS", ndts)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }
}
