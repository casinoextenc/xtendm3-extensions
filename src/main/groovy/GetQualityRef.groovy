/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT032MI.GetQualityRef
 * Description : Get records to the EXT032 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class GetQualityRef extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String NBNR

  public GetQualityRef(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {

    Integer currentCompany
    String suno = (mi.in.get("SUNO") != null ? (String)mi.in.get("SUNO") : "")
    String popn = (mi.in.get("POPN") != null ? (String)mi.in.get("POPN") : "")
    String orco = (mi.in.get("ORCO") != null ? (String)mi.in.get("ORCO") : "")
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    //Check if record exists
    DBAction queryEXT032 = database.table("EXT032")
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

    DBContainer containerEXT032 = queryEXT032.getContainer()
    containerEXT032.set("EXCONO", currentCompany)
    containerEXT032.set("EXSUNO", suno)
    containerEXT032.set("EXPOPN", popn)
    containerEXT032.set("EXORCO", orco)

    //Record exists
    if (!queryEXT032.readAll(containerEXT032, 4, outData)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  Closure<?> outData = { DBContainer containerEXT032 ->
    String supplier = containerEXT032.get("EXSUNO")
    String sigma6 = containerEXT032.get("EXPOPN")
    String countryOrig = containerEXT032.get("EXORCO")
    String countryOrigin = containerEXT032.get("EXZORI")
    String alohol = containerEXT032.get("EXZALC")
    String carac1 = containerEXT032.get("EXZCA1")
    String carac2 = containerEXT032.get("EXZCA2")
    String carac3 = containerEXT032.get("EXZCA3")
    String carac4 = containerEXT032.get("EXZCA4")
    String carac5 = containerEXT032.get("EXZCA5")
    String carac6 = containerEXT032.get("EXZCA6")
    String carac7 = containerEXT032.get("EXZCA7")
    String carac8 = containerEXT032.get("EXZCA8")
    String textID = containerEXT032.get("EXTXID")
    String storage = containerEXT032.get("EXZCON")
    String weight = containerEXT032.get("EXZPEG")
    String sanitary = containerEXT032.get("EXZSAN")
    String agreement = containerEXT032.get("EXZAGR")
    String codeIdentity = containerEXT032.get("EXZCOI")
    String phyto = containerEXT032.get("EXZPHY")
    String latin = containerEXT032.get("EXZLAT")
    String nutri = containerEXT032.get("EXZNUT")
    String Kcalori = containerEXT032.get("EXZCAL")
    String Kjoule = containerEXT032.get("EXZJOU")
    String fat = containerEXT032.get("EXZMAT")
    String fattyAcid = containerEXT032.get("EXZAGS")
    String carbohydrate = containerEXT032.get("EXZGLU")
    String sugar = containerEXT032.get("EXZSUC")
    String fiber = containerEXT032.get("EXZFIB")
    String protein = containerEXT032.get("EXZPRO")
    String salt = containerEXT032.get("EXZSEL")
    String alcoholyn = containerEXT032.get("EXZALL")
    String agreementyn = containerEXT032.get("EXZAGT")
    String quality = containerEXT032.get("EXZQUA")
    String alimental = containerEXT032.get("EXZALI")
    String entryDate = containerEXT032.get("EXRGDT")
    String entryTime = containerEXT032.get("EXRGTM")
    String changeDate = containerEXT032.get("EXLMDT")
    String changeNumber = containerEXT032.get("EXCHNO")
    String changedBy = containerEXT032.get("EXCHID")
    mi.outData.put("SUNO", supplier)
    mi.outData.put("POPN", sigma6)
    mi.outData.put("ORCO", countryOrig)
    mi.outData.put("ZORI", countryOrigin)
    mi.outData.put("ZALC", alohol)
    mi.outData.put("ZCA1",carac1)
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
}