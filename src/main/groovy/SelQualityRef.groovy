/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT032MI.SelQualityRef
 * Description : List records to the EXT032 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class SelQualityRef extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private final UtilityAPI utility


  private int currentCompany
  private int countLine
  private int nbli
  private int nbsl
  private int maxSel

  public SelQualityRef(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.logger = logger
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {
    nbli = (mi.in.get("NBLI") != null ? (Integer) mi.in.get("NBLI") : 0)
    nbsl = (mi.in.get("NBSL") != null ? (Integer) mi.in.get("NBSL") : 50)
    int desc = (mi.in.get("DESC") != null ? (Integer) mi.in.get("DESC") : 0)
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    maxSel = nbli + nbsl

    String sigma6 = (String) (mi.in.get("POPN") != null ? mi.in.get("POPN") : "")
    String supplier = (String) (mi.in.get("SUNO") != null ? mi.in.get("SUNO") : "")
    String alimental = (mi.in.get("ZALI") != null ? (Integer) mi.in.get("ZALI") : 0)
    String quality = (mi.in.get("ZQUA") != null ? (Integer) mi.in.get("ZQUA") : 0)

    ExpressionFactory ext032Expression = database.getExpressionFactory("EXT032")

    int countExpression = 0

    if (sigma6.length() > 0) {
      ext032Expression = ext032Expression.eq("EXPOPN", sigma6)
      countExpression++
    }

    if (supplier.length() > 0) {
      if (countExpression == 0) {
        ext032Expression = ext032Expression.eq("EXSUNO", supplier)
      } else {
        ext032Expression = ext032Expression.and(ext032Expression.eq("EXSUNO", supplier))
      }
      countExpression++
    }
    if (mi.in.get("ZALI") != null) {
      if (countExpression == 0) {
        ext032Expression = ext032Expression.eq("EXZALI", alimental)
      } else {
        ext032Expression = ext032Expression.and(ext032Expression.eq("EXZALI", alimental))
      }
      countExpression++
    }
    if (mi.in.get("ZQUA") != null) {
      if (countExpression == 0) {
        ext032Expression = ext032Expression.eq("EXZQUA", quality)
      } else {
        ext032Expression = ext032Expression.and(ext032Expression.eq("EXZQUA", quality))
      }
      countExpression++
    }

    if (desc == 1) {
      //Check if record exists
      DBAction ext032QueryDesc = database.table("EXT032")
        .index("20")
        .matching(ext032Expression)
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

      DBContainer ext032Request = ext032QueryDesc.getContainer()
      ext032Request.set("EXCONO", currentCompany)

      //Record exists
      if (!ext032QueryDesc.readAll(ext032Request, 1, maxSel, ext032Reader)) {
        return
      }
    } else {
      //Check if record exists
      DBAction ext032Query = database.table("EXT032")
        .index("00")
        .matching(ext032Expression)
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

      //Record exists
      if (!ext032Query.readAll(ext032Request, 1, maxSel, ext032Reader)) {
        return
      }
    }

  }

  Closure<?> ext032Reader = { DBContainer ext032Result ->
    countLine++
    if (countLine > nbli && countLine <= maxSel) {
      String supplier = ext032Result.get("EXSUNO")
      String sigma6 = ext032Result.get("EXPOPN")
      String countryOrig = ext032Result.get("EXORCO")
      String countryOrigin = ext032Result.get("EXZORI")
      String alohol = ext032Result.get("EXZALC")
      String carac1 = ext032Result.get("EXZCA1")
      String carac2 = ext032Result.get("EXZCA2")
      String carac3 = ext032Result.get("EXZCA3")
      String carac4 = ext032Result.get("EXZCA4")
      String carac5 = ext032Result.get("EXZCA5")
      String carac6 = ext032Result.get("EXZCA6")
      String carac7 = ext032Result.get("EXZCA7")
      String carac8 = ext032Result.get("EXZCA8")
      String textID = ext032Result.get("EXTXID")
      String storage = ext032Result.get("EXZCON")
      String weight = ext032Result.get("EXZPEG")
      String sanitary = ext032Result.get("EXZSAN")
      String agreement = ext032Result.get("EXZAGR")
      String codeIdentity = ext032Result.get("EXZCOI")
      String phyto = ext032Result.get("EXZPHY")
      String latin = ext032Result.get("EXZLAT")
      String nutri = ext032Result.get("EXZNUT")
      String Kcalori = ext032Result.get("EXZCAL")
      String Kjoule = ext032Result.get("EXZJOU")
      String fat = ext032Result.get("EXZMAT")
      String fattyAcid = ext032Result.get("EXZAGS")
      String carbohydrate = ext032Result.get("EXZGLU")
      String sugar = ext032Result.get("EXZSUC")
      String fiber = ext032Result.get("EXZFIB")
      String protein = ext032Result.get("EXZPRO")
      String salt = ext032Result.get("EXZSEL")
      String alcoholyn = ext032Result.get("EXZALL")
      String agreementyn = ext032Result.get("EXZAGT")
      String quality = ext032Result.get("EXZQUA")
      String alimental = ext032Result.get("EXZALI")
      String entryDate = ext032Result.get("EXRGDT")
      String entryTime = ext032Result.get("EXRGTM")
      String changeDate = ext032Result.get("EXLMDT")
      String changeNumber = ext032Result.get("EXCHNO")
      String changedBy = ext032Result.get("EXCHID")
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
    }
    if (countLine > maxSel) {
      return
    }


  }
}
