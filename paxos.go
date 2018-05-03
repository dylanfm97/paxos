package main

import(
	"log"
	"strings"
	"bufio"
	"os"
	"net/rpc"
	//"net"
	"net/http"
	"flag"
	"math/rand"
	"strconv"
	//"fmt"
	//"math/big"
	//"crypto/sha1"
	"time"
	//"math"
)


type handler func(*Node)
type Server chan<- handler

type Nothing struct{}

var gPort string
var gAddress string

type Sequence struct{
	Sequence int
	Address string
}

type Slot struct{
	Id int
	Com Com
	Sequence Sequence
	Decided bool
}

type Promise struct{
	Okay bool
	Promised int
	Com string
}

type Input struct{
	Slot int
	Sequence Sequence
	Com Com
}

type Node struct {
	Address string
	Db map[string]string
	Replicas []string
	Slots []Slot
	Listeners map[string]chan string
}

type Com struct{
	Sequence Sequence
	Com string
	Address string
	Tag int
	Key string
}



var gState *Node

func (elt Sequence) String() string { return string(elt.Sequence) }

func (this Sequence) Cmp(that Sequence) int { 
	if this.Sequence < that.Sequence {
		return -1
	} else if this.Sequence == that.Sequence{
		return 0
	} else{
		return 1
	}
}


func parse_input(){
	//var port string
	//log.Println("Paxos dist")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan(){
		line := scanner.Text()
		line = strings.TrimSpace(line)
		parts := strings.SplitN(line, " ", 2)

		if len(parts) > 1{
			parts[1] = strings.TrimSpace(parts[1])
		}

		if len(parts) == 0{
			continue
		}

		switch parts[0] {
			case "help":
				log.Println("Coms are help, port, get, put, delete, quit")

			case "put":

				/*var new_Com Com
				new_Com.Com = strings.Join(parts, " ")
				new_Com.Address = gAddress
				new_Com.Tag = rand.Intn(100)
				new_Com.Key = strconv.Itoa(new_Com.Tag) + new_Com.Address
				*/
				Sequence := Sequence{0, gAddress}

				Tag := rand.Intn(100)
				new_Com := Com { Sequence, 
								strings.Join(parts, " "),
								gAddress,
								Tag,
								strconv.Itoa(Tag) + gAddress}

				response_channel := make(chan string, 1)

				//get access to my response channel
				gState.Listeners[new_Com.Key] = response_channel
				//log.Println("Am I making it this far?")
				var response Promise

				log.Println("The Address I'm about to propose call:", gAddress)

				if err := call(gAddress, "Node.Propose", new_Com, &response); err != nil{
					log.Fatalf("calling Node.Propose: %v", err)
				}

				//log.Println("This is going to put some stuff")
			case "get":
				Sequence := Sequence{0, gAddress}

				Tag := rand.Intn(100)
				new_Com := Com { Sequence,
								strings.Join(parts, " "),
								gAddress,
								Tag,
								strconv.Itoa(Tag) + gAddress}

				response_channel := make(chan string, 1)
				gState.Listeners[new_Com.Key] = response_channel
				var response Promise
				if err := call(gAddress, "Node.Propose", new_Com, &response); err != nil{
					log.Fatalf("calling Node.Propose: %v", err)
				} 

			case "delete":
				Sequence := Sequence{0, gAddress}

				Tag := rand.Intn(100)
				new_Com := Com { Sequence,
								strings.Join(parts, " "),
								gAddress,
								Tag,
								strconv.Itoa(Tag) + gAddress}

				response_channel := make(chan string, 1)
				gState.Listeners[new_Com.Key] = response_channel
				var response Promise
				if err := call(gAddress, "Node.Propose", new_Com, &response); err != nil{
					log.Fatalf("calling Propose: %v", err)
				}

			case "dump":
				var junk Nothing
				var reply Nothing
				log.Println("The Address that I'm about to call:", gAddress)
				if err := call(gAddress, "Node.Dump", &junk, &reply); err != nil{
					log.Fatalf("calling Node.Dump: %v", err)
				}
		}
	}
}

//rpc calls --------------------------------------------------------------------

func (n *Node) Dump(junk *Nothing, reply *Nothing) error {
		log.Println("Address:", n.Address)

		log.Println("Database:")
		for Key,value := range(n.Db){
			log.Println("          ",Key, value)
		}

		log.Println("Replicas:")
		for _,elt := range(n.Replicas){
			log.Println("          ",elt)
		}


		log.Println("Slots:")
		for _,elt := range(n.Slots){
			if elt.Com.Com != "" {
				log.Println("          ",elt.Id, elt.Com.Com)
			}
		}
	return nil
}

//>>>ACCEPTOR<<<//
func (n *Node) Prepare(input *Input, reply *Promise) error{
		//go func(){
		//my sequence is the sequence that is already in the slot which
		//I'm being told not to promised anything higher than seq about
		var my_Sequence Sequence
		
		my_Sequence = n.Slots[input.Slot].Sequence
		//if the sequence number I already have is less than the one being passed in
		log.Println("the command being passed in:", input.Com.Com)
		log.Println("My sequence number for this slot:", my_Sequence.Sequence)
		log.Println("The sequence number being passed in:", input.Sequence.Sequence)
		if my_Sequence.Sequence < input.Sequence.Sequence {

			log.Println("Making Promised")
			//then replace my sequence number with the one being passed in
			n.Slots[input.Slot].Sequence.Sequence = input.Sequence.Sequence
			

			okay := true
			Promised := input.Sequence.Sequence
			Com := input.Com
			*reply = Promise{
				okay,
				Promised,
				Com.Com }
		} else {

			log.Println("NO. I already have a higher sequence number for this slot")
			okay := false
			//If I'm not taking this command, then I'm keepin the one I already have
			Promised := n.Slots[input.Slot].Sequence.Sequence
			Com := input.Com
			*reply = Promise{
				okay,
				Promised,
				Com.Com }
			}
		//}()	

	return nil
}


func (n *Node) Accept(input *Input, reply *Promise) error {
		var my_Sequence Sequence
		//the current Sequenceuence number that is in my slot
		my_Sequence = n.Slots[input.Slot].Sequence

		if my_Sequence.Sequence <= input.Sequence.Sequence || my_Sequence.Sequence == -1{
			n.Slots[input.Slot].Com = input.Com
			okay := true
			Promised := n.Slots[input.Slot].Sequence.Sequence
			Com := ""
			*reply = Promise {
				okay,
				Promised,
				Com }
		} else{
			okay := false
			Promised := n.Slots[input.Slot].Sequence.Sequence
			Com := ""
			*reply = Promise {
				okay,
				Promised,
				Com }
		}

	return nil
}

//>>>LEARNER<<<//
func (n *Node) Decide(input *Input, reply *Nothing) error {

	log.Println("Decision Made")
	log.Println("The slot:", input.Slot)
	log.Println("The Command:", input.Com.Com)

	if n.Slots[input.Slot].Decided {
		log.Fatal("Already Decided")
//			return nil
	}


//	log.Println("The command I'm saving:", input.Com.Com)
//	n.Slots[input.Slot].Com.Com = input.Com.Com

	//if this slot has already been Decided and I have a different Com
	if n.Slots[input.Slot].Decided && n.Slots[input.Slot].Com != input.Com {
		log.Fatal("Already Decided, different Com")
//			return nil
	}

	_, there := n.Listeners[input.Com.Key]
	if there {
		n.Listeners[input.Com.Key] <- input.Com.Com
	}

	//accept the Com
	my_Com := strings.Split(input.Com.Com, " ")
	n.Slots[input.Slot].Decided = true
	n.Slots[input.Slot].Com.Com = input.Com.Com


	//actually do the Com
	switch my_Com[0] {
	case "put":
		//log.Println("Put", my_Com[1], my_Com[2])
		n.Db[my_Com[1]] = my_Com[2]

	case "get":
		log.Println("Get", my_Com[1],":", n.Db[my_Com[1]])

	case "delete":
		log.Println("delete",my_Com[1])
		delete(n.Db, my_Com[1])

	}
	
	return nil
}


//>>>PROPOSER<<<//
func (n *Node) Propose(input Com, reply *Promise) error {
		//round := 1]]
	time.Sleep(3 * time.Second)

	log.Println("This Com I just got:", input)

	this_slot := 0

	highest_Promised := 0

	var Sequence Sequence
	Sequence.Address = gAddress
	log.Println("proposing...")

	log.Println("The Com being passed in:", input.Com)

	log.Println("The Address being passed in:", input.Address)
	//find the first unDecided slot that I know of
	log.Println("The Tag being passe in:", input.Tag)

	log.Println("the key being passed in:", input.Key)

	for {
		for i, elt := range(n.Slots){
			if !elt.Decided {
				this_slot = i
				break
			}
		}		

		log.Println("The slot we're dealing with:", this_slot)
		//create my slot 
	//	var new_slot Slot
		//get the Sequenceuence number of the Com that is being passed in
	//	new_slot.Sequence = input.Sequence

	//	log.Println("the Sequenceuence number of the Com that is being passed in ", new_slot.Sequence)
		


		Sequence_number := highest_Promised + 1
		Sequence.Sequence = Sequence_number
		//if this is the first Com that is being issued...
		//set the Com of my slot to the comman that is being passed in
		//new_slot.Com.Com = input.Com

		//new_slot.id = this_slot
		//set the Sequenceuence of the Com in the slot to the Sequence of the new slot
		//new_slot.Com.Sequence.Sequence = new_slot.Sequence.Sequence

		log.Println("slot:", this_slot, "Sequence_number:", Sequence_number, "Com:", input.Com)
		req := Input {
			this_slot,
			Sequence,
			input }

		var okay bool
		var Promised int
		var Com string

		resp := Promise {
			okay,
			Promised,
			Com }

		responses := make(chan Promise)                          
		//do a prepare request to each cell
		//store all the responses in a channel			
		
		for _,elt := range(n.Replicas){
			go func(elt string, responses chan Promise){
					log.Println("Calling prepare on:", elt)
					call(elt, "Node.Prepare", &req, &resp)
					//log.Println("Am I making it this far ???????") 
					responses <- resp
			}(elt, responses)
		}

		//look at all the responses and get the one with the highest Promised number
		promises := 0
		rejections := 0

		var highest_Com string

		log.Println("The responses:")


		for i := 0; i < len(n.Replicas); i++ {
	
			this_response := <-responses

			log.Println("okay:", this_response.Okay)
			log.Println("Promised:", this_response.Promised)
			log.Println("Command:", this_response.Com)

			if this_response.Okay {
				promises ++
			} else{
				rejections ++
			}

			if this_response.Okay && this_response.Promised > highest_Promised {
				highest_Promised = this_response.Promised
				highest_Com = this_response.Com
			}		
		}


		//we have a majority if there are more promises than there are rejections
		log.Println("A majority has been reached")
		if promises > rejections {
			input.Sequence.Sequence = highest_Promised
			input.Com = highest_Com
			
			for _, elt := range(n.Replicas){
				go func(elt string){
						log.Println("calling Accept on:", elt)
					 	call(elt, "Node.Accept", &req, &resp)
					 	responses <- resp
				}(elt)
			}
			accepts := 0
			rejections = 0
			// for i := 0; i < len(n.Slots); i++ {
			 	this_response := <-responses
			 	if this_response.Okay {
			 		accepts++
			 	} else {
			 		rejections++
			 	}
			 	if accepts > rejections {
			 		//input.Slot = new_slot.id
			 		var junk Nothing
			 		var decided Input
			 		decided.Slot = this_slot
			 		decided.Com = input
			 		
			 			//also call decide on myself

			 		go func(){
			 			call(gAddress, "Node.Decide", &decided, &junk)
			 		}()

				 	for _, elt := range(n.Replicas){
				 		go func(elt string){
				 			
				 				log.Println("Calling Decide on:", elt)
				 				call(elt, "Node.Decide", &decided, &junk)	
				 		}(elt)
			 		}
			 		break
			 	//}
			}
		}

	}//end infinite while

	return nil
}

//-----------------------------------------------------------------------------



//node methods ----------------------->>>>>>>>>>>

func (n *Node) insert(Key string, value string){
		n.Db[Key] = value
}

//------------------------------------>>>>>>>>>>>

func call(Address string, method string, request interface{}, response interface{}) error{
	client, err := rpc.DialHTTP("tcp", Address)
	if err != nil {
		log.Printf("rpc.DialHTTP: %v", err)
		return err
	}
	defer client.Close()

	if err = client.Call(method, request, response); err != nil {
		log.Fatalf("client.Call %s: %v", method, err)
		return err
	}
	return nil
}

func createNode(Address string, Replicas []string) *Node{
	var Db = make(map[string]string)
	var slots = make([]Slot, 100)
	for i,_ := range(slots){
		slots[i].Id = i
	}
	var Listeners = make(map[string]chan string, 1)
	return &Node{ 
		Address, 
		Db,
		Replicas,
		slots,
		Listeners }


}

func startActor(Address string, Replicas []string) Server {
	ch := make(chan handler)
	gState = createNode(Address, Replicas)
	go func() {
		for f := range ch {
			f(gState) 
		}
	}()
	return ch
}

func serve(Address string, Replicas []string){
	//actor := startActor(Address, Replicas)

	gState = createNode(Address, Replicas)
	rpc.Register(gState)
	rpc.HandleHTTP()

	go func() {
		err := http.ListenAndServe(Address, nil)
		if err != nil {
			log.Println(err.Error())
			os.Exit(1)
		}
	}()

/*
	log.Println("my_Address", Address)
	rpc.Register(actor)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", Address)
	if e != nil {
	}
	if err := http.Serve(l, nil); err != nil{

	}	

*/
}

func main(){
	//var port string
	var ports = make([]string, 0)

	flag.Parse()

	switch flag.NArg(){
	case 0:
		gPort = "3410"
		log.Println("there are not arguments")

	case 1:
		gPort = flag.Arg(0)

	case 2:
		gPort = flag.Arg(0)

		temp := "localhost:"
		temp += flag.Arg(1)

		ports = append(ports, temp)

	case 3:
		gPort = flag.Arg(0)

		temp1 := "localhost:"
		temp1 += flag.Arg(1)
		temp2 := "localhost:"
		temp2 += flag.Arg(2)

		ports = append(ports, temp1)
		ports = append(ports, temp2)
		//log.Println("stuff is being appended")
	
	case 4:
		gPort = flag.Arg(0)

		temp1 := "localhost:"
		temp1 += flag.Arg(1)
		temp2 := "localhost:"
		temp2 += flag.Arg(2)
		temp3 := "localhost:"
		temp3 += flag.Arg(3)

		ports = append(ports, temp1)
		ports = append(ports, temp2)
		ports = append(ports, temp3)
		//log.Println("stuff is being appended")
	
	case 5:
		gPort = flag.Arg(0)

		temp1 := "localhost:"
		temp1 += flag.Arg(1)
		temp2 := "localhost:"
		temp2 += flag.Arg(2)
		temp3 := "localhost:"
		temp3 += flag.Arg(3)
		temp4 := "localhost:"
		temp4 += flag.Arg(4)

		ports = append(ports, temp1)
		ports = append(ports, temp2)
		ports = append(ports, temp3)
		ports = append(ports, temp4)

}



	go func(){
		gAddress = "localhost:" + gPort
		serve(gAddress, ports)	
	}()

	//for _,elt := range ports {
		//log.Println("this is an element of the slice", elt)
	//}

	parse_input()
	
}









