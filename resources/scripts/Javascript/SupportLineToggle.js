MutationObserver = window.MutationObserver || window.WebKitMutationObserver;

var targetDomId = "drop"
var onLoadVal=$('#'+targetDomId+' .ComboBoxTextDivContainer').text();
 
if(onLoadVal=="Single Node")
{
	$("#collection").hide()
	$(".subnetwork").hide()
	$(".singleNode").show()
	}
else if(onLoadVal=="Collection")
	{
		$(".singleNode").hide()
		$(".subnetwork").hide()
	    $("#collection").show()
	}
    else 
	{
		$(".singleNode").hide()
		$(".subnetwork").show()
		$("#collection").hide()
	}
		
	

var myFunction = function(oldValue,newValue)
{
  if(newValue=="Single Node")
  {
	$("#collection").hide()
	$(".subnetwork").hide()
	$(".singleNode").show()
	}
	else if(newValue=="Collection")
	{
		$(".singleNode").hide()
		$(".subnetwork").hide()
		$("#collection").show()
		}
	else
	{
		$(".singleNode").hide()
		$(".subnetwork").show()
		$("#collection").hide()
	}
}
        

var target = document.getElementById(targetDomId)
var oldVal = target.innerText.trim()

var callback = function(mutations) {
 newVal=$('#'+targetDomId+' .ComboBoxTextDivContainer').text()
 if(newVal!=oldVal) myFunction(oldVal,newVal)
 oldVal = newVal;
}

var observer = new MutationObserver(callback);

var opts = {
    childList: true, 
    attributes: true, 
    characterData: true, 
    subtree: true
}

observer.observe(target,opts);