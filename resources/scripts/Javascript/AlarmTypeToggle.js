MutationObserver = window.MutationObserver || window.WebKitMutationObserver;
var targetAlarmTypeId = "alarmtype"
var onLoadAlarmTypeVal=$('#'+targetAlarmTypeId+' .ComboBoxTextDivContainer').text();
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

var toggleInputs = function(oldValue,newValue){
	if(newValue=="Threshold" || newValue=="Case Dependent Threshold"){
		$(".lookback").hide()
		$(".datarange").hide()
	}
	else if(newValue=="Past Comparison Detection"){
		$(".lookback").show()
		$(".datarange").hide()
	}
	else if(newValue=="Continuous Detection" || newValue=="Trend" || newValue=="Dynamic Threshold"){
		$(".lookback").hide()
		$(".datarange").show()
	}
	else if(newValue=="Past Comparison Detection + Continuous Detection"){
		$(".lookback").show()
		$(".datarange").show()
	}
}

var alarmTypeTarget = document.getElementById(targetAlarmTypeId)
var oldAlarmTypeVal = alarmTypeTarget.innerText.trim()

var callback = function(mutations) {
    newAlarmVal=$('#'+targetAlarmTypeId+' .ComboBoxTextDivContainer').text()
    if(newAlarmVal!=oldAlarmTypeVal) toggleInputs(oldAlarmTypeVal,newAlarmVal)
    oldAlarmTypeVal = newAlarmVal;
   }
   
   var observer = new MutationObserver(callback);
   
   var opts = {
       childList: true, 
       attributes: true, 
       characterData: true, 
       subtree: true
   }
   
   observer.observe(alarmTypeTarget,opts);