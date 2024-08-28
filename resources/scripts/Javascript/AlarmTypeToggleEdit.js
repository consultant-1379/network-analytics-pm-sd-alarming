var alarmType = {
    "cdt": "Case Dependent Threshold",
    "dynamic": "Dynamic Threshold",
	"cd": "Continuous Detection",
	"pcd": "Past Comparison Detection",
	"pcd+cd": "Past Comparison Detection + Continuous Detection",
	"threshold": "Threshold",
	"trend": "Trend"
};

var getProperty = function (propertyName) {
    return alarmType[propertyName];
};

var targetAlarmTypeId = "alarm_type";
var currentAlarmType = $('#'+targetAlarmTypeId).text();
var onLoadAlarmTypeVal=getProperty(currentAlarmType);

if(onLoadAlarmTypeVal=="Past Comparison Detection"){
	$(".lookback").show()
	$(".datarange").hide()
}
else if(onLoadAlarmTypeVal=="Continuous Detection" || onLoadAlarmTypeVal=="Trend" || onLoadAlarmTypeVal=="Dynamic Threshold"){
	$(".lookback").hide()
	$(".datarange").show()
}
else if(onLoadAlarmTypeVal=="Past Comparison Detection + Continuous Detection"){
	$(".lookback").show()
	$(".datarange").show()
}
else {
    $(".lookback").hide()
	$(".datarange").hide()
}

var kpi1 = $("#kpi1input").text().trim()
var kpi2 = $("#kpi2input").text().trim()
var kpi3 = $("#kpi3input").text().trim()
var kpi4 = $("#kpi4input").text().trim()
console.log("------------------",$("#kpi1input").text().trim())
if(kpi1 == 0){
	$(".kpi1").hide()
}else{
	$(".kpi1").show()
}
if(kpi2 == 0){
	$(".kpi2").hide()
}else{
	$(".kpi2").show()
}
if(kpi3 == 0){
	$(".kpi3").hide()
}else{
	$(".kpi3").show()
}
if(kpi4 == 0){
	$(".kpi4").hide()
}else{
	$(".kpi4").show()
}
$('#alarm_type_input input').val(getProperty(currentAlarmType)).blur()