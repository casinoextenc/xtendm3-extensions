/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT020MI.GetAssortCriter
 * Description : The GetAssortCriter transaction get records to the EXT020 table.
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 */
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
public class GetAssortCriter extends ExtendM3Transaction {
	private final MIAPI mi;
	private final DatabaseAPI database
	private final LoggerAPI logger
	private final MICallerAPI miCaller;
	private final ProgramAPI program;
	private final UtilityAPI utility;

	public GetAssortCriter(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility) {
		this.mi = mi;
		this.database = database;
		this.logger = logger;
		this.program = program;
		this.utility = utility;
	}

	public void main() {
		Integer currentCompany;
		String cuno = "";
		String ascd = "";
		String fdat ="";
		if (mi.in.get("CONO") == null) {
			currentCompany = (Integer)program.getLDAZD().CONO;
		} else {
			currentCompany = mi.in.get("CONO");
		}

		if(mi.in.get("CUNO") != null){
			DBAction countryQuery = database.table("OCUSMA").index("00").build();
			DBContainer OCUSMA = countryQuery.getContainer();
			OCUSMA.set("OKCONO",currentCompany);
			OCUSMA.set("OKCUNO",mi.in.get("CUNO"));
			if (!countryQuery.read(OCUSMA)) {
				mi.error("Code Client " + mi.in.get("CUNO") + " n'existe pas");
				return;
			}
			cuno = mi.in.get("CUNO");
		}else{
			mi.error("Code Client est obligatoire");
			return;
		}

		if(mi.in.get("ASCD") != null){
			DBAction countryQuery = database.table("CSYTAB").index("00").build();
			DBContainer CSYTAB = countryQuery.getContainer();
			CSYTAB.set("CTCONO",currentCompany);
			CSYTAB.set("CTSTCO",  "ASCD");
			CSYTAB.set("CTSTKY", mi.in.get("ASCD"));
			if (!countryQuery.read(CSYTAB)) {
				mi.error("Code Assortiment  " + mi.in.get("ASCD") + " n'existe pas");
				return;
			}
			ascd = mi.in.get("ASCD");
		}else{
			mi.error("Code Assortiment  " + mi.in.get("ASCD") + " n'existe pas");
			return;
		}

		if(mi.in.get("FDAT") != null){
			fdat = mi.in.get("FDAT");
			if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
				mi.error("Format Date de Validité incorrect");
				return;
			}
		}else{
			mi.error("Date de Validité est obligatoire");
			return;
		}
		LocalDateTime timeOfCreation = LocalDateTime.now();
		DBAction query = database.table("EXT020").index("00").selection("EXCONO", "EXASCD", "EXCUNO", "EXFDAT", "EXSTAT", "EXSTTS", "EXNDTS", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
		DBContainer EXT020 = query.getContainer();
		EXT020.set("EXCONO", currentCompany);
		EXT020.set("EXCUNO", cuno);
		EXT020.set("EXASCD", ascd);
		EXT020.setInt("EXFDAT", fdat as Integer);
		if(!query.readAll(EXT020, 4, outData)){
			mi.error("L'enregistrement existe déjà");
			return;
		}
	}

	Closure<?> outData = { DBContainer EXT020 ->
		String cono = EXT020.get("EXCONO");
		String cuno = EXT020.get("EXCUNO");
		String ascd = EXT020.get("EXASCD");
		String fdat = EXT020.get("EXFDAT");
		String stat = EXT020.get("EXSTAT");
		String stts = EXT020.get("EXSTTS");
		String ndts = EXT020.get("EXNDTS");
		String entryDate = EXT020.get("EXRGDT");
		String entryTime = EXT020.get("EXRGTM");
		String changeDate = EXT020.get("EXLMDT");
		String changeNumber = EXT020.get("EXCHNO");
		String changedBy = EXT020.get("EXCHID");

		mi.outData.put("CONO", cono);
		mi.outData.put("CUNO", cuno);
		mi.outData.put("ASCD", ascd);
		mi.outData.put("FDAT", fdat);
		mi.outData.put("STAT", stat);
		mi.outData.put("STTS", stts);
		mi.outData.put("NDTS", ndts);
		mi.outData.put("RGDT", entryDate);
		mi.outData.put("RGTM", entryTime);
		mi.outData.put("LMDT", changeDate);
		mi.outData.put("CHNO", changeNumber);
		mi.outData.put("CHID", changedBy);
		mi.write();
	}
}