/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT032MI.SelQualityRef
 * Description : List records to the EXT032 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class SelQualityRef extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
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

    Integer currentCompany
    String suno = (mi.in.get("SUNO") != null ? (String)mi.in.get("SUNO") : "")
    String popn = (mi.in.get("POPN") != null ? (String)mi.in.get("POPN") : "")
    String orco = (mi.in.get("ORCO") != null ? (String)mi.in.get("ORCO") : "")
    nbli = (mi.in.get("NBLI") != null ? (Integer)mi.in.get("NBLI") : 0)
    nbsl = (mi.in.get("NBSL") != null ? (Integer)mi.in.get("NBSL") : 50)
    int desc = (mi.in.get("DESC") != null ? (Integer)mi.in.get("DESC") : 0)
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    
    maxSel = nbli + nbsl

    LocalDateTime timeOfCreation = LocalDateTime.now()
    String sigma6 =  (String)(mi.in.get("POPN") != null ? mi.in.get("POPN") : "")
    String supplier = (String)(mi.in.get("SUNO") != null ? mi.in.get("SUNO") : "")
    String alimental = (mi.in.get("ZALI") != null ? (Integer)mi.in.get("ZALI") : 0)
    String quality = (mi.in.get("ZQUA") != null ? (Integer)mi.in.get("ZQUA") : 0)
    ExpressionFactory expression = database.getExpressionFactory("EXT032")

    int countExpression = 0
    
    if (sigma6.length() > 0) {
      expression = expression.eq("EXPOPN", sigma6)
      countExpression++
    }
    
    if (supplier.length() > 0) {
      if (countExpression == 0) {
        expression = expression.eq("EXSUNO", supplier)
      } else {
        expression = expression.and(expression.eq("EXSUNO", supplier))
      }
      countExpression++
    }
    if (mi.in.get("ZALI") != null) {
      if (countExpression == 0) {
        expression = expression.eq("EXZALI", alimental)
      } else {
        expression = expression.and(expression.eq("EXZALI", alimental))
      }
      countExpression++
    }
    if (mi.in.get("ZQUA") != null) {
      if (countExpression == 0) {
        expression = expression.eq("EXZQUA", quality)
      } else {
        expression = expression.and(expression.eq("EXZQUA", quality))
      }
      countExpression++
    }
   
    if (desc == 1) {
      //Check if record exists
      DBAction queryEXT032Desc = database.table("EXT032")
          .index("20")
          .matching(expression)
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
  
      DBContainer containerEXT032 = queryEXT032Desc.getContainer()
      containerEXT032.set("EXCONO", currentCompany)
  
      //Record exists
      if (!queryEXT032Desc.readAll(containerEXT032, 1, outData)){
        //mi.error("L'enregistrement n'existe pas")
        return
      }
    } else {
      //Check if record exists
      DBAction queryEXT032 = database.table("EXT032")
          .index("00")
          .matching(expression)
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
  
      //Record exists
      if (!queryEXT032.readAll(containerEXT032, 1, outData)){
        //mi.error("L'enregistrement n'existe pas")
        return
      }
    }

  }

  Closure<?> outData = { DBContainer containerEXT032 ->
    countLine++
    if (countLine > nbli && countLine <= maxSel) {
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
    if (countLine > maxSel) {
      return
    }
    
    
  }
}