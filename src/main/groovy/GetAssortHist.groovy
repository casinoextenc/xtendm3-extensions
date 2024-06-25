/**
 * This extension is used by Mashup
 * Name : EXT021MI.GetAssortHist
 * COMX01 Gestion des assortiments clients
 * Description : The GetAssortHist transaction get records to the EXT021 table.
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */
public class GetAssortHist extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  public GetAssortHist(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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

    if (mi.in.get("TYPE") != null) {
      type = mi.in.get("TYPE")
    } else {
      mi.error("Type est obligatoire")
      return
    }
    if (mi.in.get("DATA") != null) {
      data = mi.in.get("DATA")
    } else {
      mi.error("data est obligatoire")
      return
    }

    DBAction ext021Query = database.table("EXT021").index("00").selection("EXCONO", "EXASCD", "EXCUNO", "EXFDAT", "EXTYPE", "EXCHB1", "EXDATA", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID", "EXTX60").build()
    DBContainer ext021Request = ext021Query.getContainer()
    ext021Request.set("EXCONO", currentCompany)
    ext021Request.set("EXCUNO", cuno)
    ext021Request.set("EXASCD", ascd)
    ext021Request.setInt("EXFDAT", fdat as Integer)
    ext021Request.set("EXTYPE", type)
    ext021Request.set("EXDATA", data)
    if (ext021Query.read(ext021Request)) {
      mi.outData.put("CONO", ext021Request.get("EXCONO") as String)
      mi.outData.put("CUNO", ext021Request.get("EXCUNO") as String)
      mi.outData.put("ASCD", ext021Request.get("EXASCD") as String)
      mi.outData.put("FDAT", ext021Request.get("EXFDAT") as String)
      mi.outData.put("CHB1", ext021Request.get("EXCHB1") as String)
      mi.outData.put("TYPE", ext021Request.get("EXTYPE") as String)
      mi.outData.put("DATA", ext021Request.get("EXDATA") as String)
      mi.outData.put("TX60", ext021Request.get("EXTX60") as String)
      mi.outData.put("RGDT", ext021Request.get("EXRGDT") as String)
      mi.outData.put("RGTM", ext021Request.get("EXRGTM") as String)
      mi.outData.put("LMDT", ext021Request.get("EXLMDT") as String)
      mi.outData.put("CHNO", ext021Request.get("EXCHNO") as String)
      mi.outData.put("CHID", ext021Request.get("EXCHID") as String)
      mi.write()
    } else {
      mi.error("L'enregistrement existe déjà")
      return
    }
  }
}
