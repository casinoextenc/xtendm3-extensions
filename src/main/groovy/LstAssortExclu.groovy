/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT025MI.LstAssortExclu
 * Description : The LstAssortExclu transaction get records to the EXT025 table.
 * Date         Changed By   Description
 * 20240206     YVOYOU     COMX01 - Assortment
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class LstAssortExclu extends ExtendM3Transaction {
	private final MIAPI mi
	private final DatabaseAPI database
	private final LoggerAPI logger
	private final MICallerAPI miCaller
	private final ProgramAPI program
	private final UtilityAPI utility

	public LstAssortExclu(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility) {
		this.mi = mi
		this.database = database
		this.logger = logger
		this.program = program
		this.utility = utility
	}

	public void main() {
		Integer currentCompany
		String cuno = ""
		String itno = ""
		String fdat =""
		if (mi.in.get("CONO") == null) {
			currentCompany = (Integer)program.getLDAZD().CONO
		} else {
			currentCompany = mi.in.get("CONO")
		}
		cuno = mi.in.get("CUNO")
		itno = mi.in.get("ITNO")
		
		if(mi.in.get("FDAT") != null){
			fdat = mi.in.get("FDAT")
			if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
				mi.error("Format Date de Validité incorrect")
				return
			}
		}
		//Create Expression
		 ExpressionFactory expression = database.getExpressionFactory("EXT025");
		 expression = expression.eq("EXCONO", currentCompany.toString());
		 if(cuno!=""){
		   expression =  expression.and(expression.ge("EXCUNO", cuno));
		 }
		 if(itno!="") {
		   expression = expression.and(expression.ge("EXITNO", itno));
		 }
		 if(fdat!="") {
		   expression = expression.and(expression.ge("EXFDAT", fdat));
		 }
		 //Run Select
		 DBAction query = database.table("EXT025").index("00").matching(expression).selection("EXCONO", "EXITNO", "EXCUNO", "EXFDAT", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build();
		 DBContainer EXT025 = query.getContainer();
		 EXT025.setInt("EXCONO",currentCompany);
		 if(!query.readAll(EXT025, 1, outData)){
		   mi.error("L'enregistrement n'existe pas");
		   return;
		 }
	}

	Closure<?> outData = { DBContainer EXT025 ->
		String cono = EXT025.get("EXCONO")
		String cuno = EXT025.get("EXCUNO")
		String itno = EXT025.get("EXITNO")
		String fdat = EXT025.get("EXFDAT")
		String entryDate = EXT025.get("EXRGDT")
		String entryTime = EXT025.get("EXRGTM")
		String changeDate = EXT025.get("EXLMDT")
		String changeNumber = EXT025.get("EXCHNO")
		String changedBy = EXT025.get("EXCHID")

		mi.outData.put("CONO", cono)
		mi.outData.put("CUNO", cuno)
		mi.outData.put("ITNO", itno)
		mi.outData.put("FDAT", fdat)
		mi.outData.put("RGDT", entryDate)
		mi.outData.put("RGTM", entryTime)
		mi.outData.put("LMDT", changeDate)
		mi.outData.put("CHNO", changeNumber)
		mi.outData.put("CHID", changedBy)
		mi.write()
	}
}
