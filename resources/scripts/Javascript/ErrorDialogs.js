
$("#editBtn").click(function() {
	AlarmState = $("#ErrorInput").first().text().trim();
	isMarked = $("#isMarked").first().text().trim();
	
	if(isMarked=="True"){
		if(AlarmState=="Inactive"){
			$("#editBtnInput input").val('Editable').blur();
		}else{
			createNotInactiveDialog()
		}
	}	
	else{
		createNoMarkingsConfirmDialog()
	}
});






$("#deactivateBtn").click(function() {
	isMarked = $("#isMarked").first().text().trim();
	if(isMarked=="True"){
		$("#deactivatetBtnInput input").val('Other').blur();
	}else{
		createNoMarkingsConfirmDialog()
	}
	
});




$("#activateBtn").click(function() {
	isMarked = $("#isMarked").first().text().trim();
	if(isMarked=="True"){
		$("#activateBtnInput input").val('Other').blur();
	}else{
		createNoMarkingsConfirmDialog()
	}
	
});





$("#deleteBtn").click(function() {
	AlarmState = $("#ErrorInput").first().text().trim();
	isMarked = $("#isMarked").first().text().trim();
	if(isMarked=="True"){
		if(AlarmState=="Inactive"){
			createConfirmDeleteDialog()
		}else{
			createNotInactiveDialog()
		}
	}
	else{
		createNoMarkingsConfirmDialog()
	}
});
  
  
  
  
function createConfirmDeleteDialog(){
	$( "#dialog-confirm" ).dialog({
      resizable: false,
      height: "auto",
      width: 400,
      modal: true,
	  draggable: false,
      buttons: {
        "Delete alarm": function() {
          $( this ).dialog( "close" );
		  $("#deleteBtnInput input").val('Other').blur();
        },
        Cancel: function() {
          $( this ).dialog( "close" );
        }
      }
    });
}

function createNotInactiveDialog(){ 
   $( "#dialog-error" ).dialog({
      resizable: false,
      height: "auto",
      width: 400,
      modal: true,
	  draggable: false,
      buttons: {
        "OK": function() {
          $( this ).dialog( "close" );
        }
      }
    });
}

function createNoMarkingsConfirmDialog(){ 
   $( "#dialog-noMarkings" ).dialog({
      resizable: false,
      height: "auto",
      width: 400,
      modal: true,
	  draggable: false,
      buttons: {
        "OK": function() {
          $( this ).dialog( "close" );
        }
      }
    });
}

