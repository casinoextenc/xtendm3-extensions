/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT032MI.GetQualityRef
 * Description : Get records to the EXT032 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 * 20240712     FLEBARS      QUAX01 - Controle code pour validation Infor - Retours
 */
public class GetQualityRef extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  private int currentCompany

  public GetQualityRef(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
    this.logger = logger
  }

  public void main() {
    String suno = (mi.in.get("SUNO") != null ? (String) mi.in.get("SUNO") : "")
    String popn = (mi.in.get("POPN") != null ? (String) mi.in.get("POPN") : "")
    String orco = (mi.in.get("ORCO") != null ? (String) mi.in.get("ORCO") : "")
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Check if record exists
    DBAction ext032Query = database.table("EXT032")
      .index("00")
      .selection(
        "EXSUNO",
        "EXPOPN",
        "EXORCO",
        "EXZORI",
        "EXZALC",
        "EXZCA1",
        "EXZCA2",
        "EXZCA3",
        "EXZCA4",
        "EXZCA5",
        "EXZCA6",
        "EXZCA7",
        "EXZCA8",
        "EXTXID",
        "EXZCON",
        "EXZPEG",
        "EXZSAN",
        "EXZAGR",
        "EXZCOI",
        "EXZPHY",
        "EXZLAT",
        "EXZNUT",
        "EXZCAL",
        "EXZJOU",
        "EXZMAT",
        "EXZAGS",
        "EXZGLU",
        "EXZSUC",
        "EXZFIB",
        "EXZPRO",
        "EXZSEL",
        "EXZALL",
        "EXZAGT",
        "EXZQUA",
        "EXZALI",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer ext032Request = ext032Query.getContainer()
    ext032Request.set("EXCONO", currentCompany)
    ext032Request.set("EXSUNO", suno)
    ext032Request.set("EXPOPN", popn)
    ext032Request.set("EXORCO", orco)
    //Record exists
    if (ext032Query.read(ext032Request)){
      String supplier = ext032Request.get("EXSUNO")
      String sigma6 = ext032Request.get("EXPOPN")
      String countryOrig = ext032Request.get("EXORCO")
      String countryOrigin = ext032Request.get("EXZORI")
      String alohol = ext032Request.get("EXZALC")
      String carac1 = ext032Request.get("EXZCA1")
      String carac2 = ext032Request.get("EXZCA2")
      String carac3 = ext032Request.get("EXZCA3")
      String carac4 = ext032Request.get("EXZCA4")
      String carac5 = ext032Request.get("EXZCA5")
      String carac6 = ext032Request.get("EXZCA6")
      String carac7 = ext032Request.get("EXZCA7")
      String carac8 = ext032Request.get("EXZCA8")
      String textID = ext032Request.get("EXTXID")
      String storage = ext032Request.get("EXZCON")
      String weight = ext032Request.get("EXZPEG")
      String sanitary = ext032Request.get("EXZSAN")
      String agreement = ext032Request.get("EXZAGR")
      String codeIdentity = ext032Request.get("EXZCOI")
      String phyto = ext032Request.get("EXZPHY")
      String latin = ext032Request.get("EXZLAT")
      String nutri = ext032Request.get("EXZNUT")
      String Kcalori = ext032Request.get("EXZCAL")
      String Kjoule = ext032Request.get("EXZJOU")
      String fat = ext032Request.get("EXZMAT")
      String fattyAcid = ext032Request.get("EXZAGS")
      String carbohydrate = ext032Request.get("EXZGLU")
      String sugar = ext032Request.get("EXZSUC")
      String fiber = ext032Request.get("EXZFIB")
      String protein = ext032Request.get("EXZPRO")
      String salt = ext032Request.get("EXZSEL")
      String alcoholyn = ext032Request.get("EXZALL")
      String agreementyn = ext032Request.get("EXZAGT")
      String quality = ext032Request.get("EXZQUA")
      String alimental = ext032Request.get("EXZALI")
      String entryDate = ext032Request.get("EXRGDT")
      String entryTime = ext032Request.get("EXRGTM")
      String changeDate = ext032Request.get("EXLMDT")
      String changeNumber = ext032Request.get("EXCHNO")
      String changedBy = ext032Request.get("EXCHID")
      mi.outData.put("SUNO", supplier)
      mi.outData.put("POPN", sigma6)
      mi.outData.put("ORCO", countryOrig)
      mi.outData.put("ZORI", countryOrigin)
      mi.outData.put("ZALC", alohol)
      mi.outData.put("ZCA1", carac1)
      mi.outData.put("ZCA2", carac2)
      mi.outData.put("ZCA3", carac3)
      mi.outData.put("ZCA4", carac4)
      mi.outData.put("ZCA5", carac5)
      mi.outData.put("ZCA6", carac6)
      mi.outData.put("ZCA7", carac7)
      mi.outData.put("ZCA8", carac8)
      mi.outData.put("TXID", textID)
      mi.outData.put("ZCON", storage)
      mi.outData.put("ZPEG", weight)
      mi.outData.put("ZSAN", sanitary)
      mi.outData.put("ZAGR", agreement)
      mi.outData.put("ZCOI", codeIdentity)
      mi.outData.put("ZPHY", phyto)
      mi.outData.put("ZLAT", latin)
      mi.outData.put("ZNUT", nutri)
      mi.outData.put("ZCAL", Kcalori)
      mi.outData.put("ZJOU", Kjoule)
      mi.outData.put("ZMAT", fat)
      mi.outData.put("ZAGS", fattyAcid)
      mi.outData.put("ZGLU", carbohydrate)
      mi.outData.put("ZSUC", sugar)
      mi.outData.put("ZFIB", fiber)
      mi.outData.put("ZPRO", protein)
      mi.outData.put("ZSEL", salt)
      mi.outData.put("ZALL", alcoholyn)
      mi.outData.put("ZAGT", agreementyn)
      mi.outData.put("ZQUA", quality)
      mi.outData.put("ZALI", alimental)
      mi.outData.put("RGDT", entryDate)
      mi.outData.put("RGTM", entryTime)
      mi.outData.put("LMDT", changeDate)
      mi.outData.put("CHNO", changeNumber)
      mi.outData.put("CHID", changedBy)
      mi.write()
    } else {
      mi.error("L'enregistrement n'existe pas")
    }
  }
}
