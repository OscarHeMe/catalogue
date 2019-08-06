// Doc Ready
document.addEventListener("DOMContentLoaded", function(){
    if(current_module != 'item')
        return false;

    document.getElementById("search").addEventListener('change',function(){
        new_url = url_change_id_active+"&q="+this.value;
        window.location.href=new_url;
    });

    const inputs = document.querySelectorAll('.product-input');
    const itemInputs = document.querySelectorAll('.item-input');

    // Product id input
    for (let i=0; i < inputs.length; i++){
        inputs[i].addEventListener('keyup',function(){
            console.log("Changed!!");
            console.log(this);
            //document.querySelector('#UserName').value = this.value;
            let payload = {
                "item_uuid" : this.getAttribute("item_uuid"),
                "source" : this.getAttribute("source"),
                "product_id" : this.value,
                "auth" : document.getElementById("auth-code").value
            };
            console.log(payload)
            fetch(url_change_id, {
                method : 'POST',
                cache: 'no-cache',
                headers : {
                    'Content-Type' : 'application/json'
                },
                body : JSON.stringify(payload)
            }).then(function(response){
                if(response.status != 200){
                    console.log(response)
                    feedback(response.statusText,response.status,"error");
                    return false;
                }
                return response.json(); 
            }).then(function(myJson){
                console.log(myJson);
            }).catch(function(err){
                console.log(err);
            })
        });
    }

    // Name input
    for (let i=0; i < itemInputs.length; i++){
        itemInputs[i].addEventListener('keyup',function(){
            console.log("Item changed!!");
            console.log(this);
            let payload = {};
            payload["item_uuid"] = this.getAttribute("item_uuid");
            payload["auth"] = document.getElementById("auth-code").value;
            payload[this.getAttribute("field")] = this.value;
            console.log(payload)
            fetch(url_change_item, {
                method : 'POST',
                cache: 'no-cache',
                headers : {
                    'Content-Type' : 'application/json'
                },
                body : JSON.stringify(payload)
            }).then(function(response){
                if(response.status != 200){
                    console.log(response)
                    feedback(response.statusText,response.status,"error");
                    return false;
                }
                return response.json(); 
            }).then(function(myJson){
                console.log(myJson);
            }).catch(function(err){
                console.log(err);
            })
        });
    }


});