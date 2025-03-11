package choice_multi

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"

	"go.temporal.io/sdk/activity"
	"google.golang.org/api/deploymentmanager/v2"
)

type OrderActivities struct {
	OrderChoices []string
}

func (a *OrderActivities) GetOrder() (string, error) {
	idx := rand.Intn(len(a.OrderChoices))
	order := a.OrderChoices[idx]
	fmt.Printf("Order is for %s\n", order)
	return order, nil
}

func (a *OrderActivities) OrderApple(choice string) error {
	fmt.Printf("Order choice-multi1111: %v\n", choice)
	ctx := context.Background()
	deploymentmanagerService, err := deploymentmanager.NewService(ctx)
	if err != nil {
		return err
	}

	projectId := "478271815769" //ua097ba39031f53a4-tp
	content, err := os.ReadFile("vsa_config/sample_yaml.yaml")
	if err != nil {
		log.Println(err)
		return err
	}
	configFile := deploymentmanager.ConfigFile{Content: string(content)}
	f1Content, err := os.ReadFile("vsa_config/netapp-cvo-deployment.py")
	if err != nil {
		log.Println(err)
		return err
	}
	f2Content, err := os.ReadFile("vsa_config/netapp-cvo-deployment.py.schema")
	if err != nil {
		log.Println(err)
		return err
	}
	file1 := deploymentmanager.ImportFile{Name: "netapp-cvo-deployment.py", Content: string(f1Content)}
	file2 := deploymentmanager.ImportFile{Name: "netapp-cvo-deployment.py.schema", Content: string(f2Content)}
	imports := []*deploymentmanager.ImportFile{&file1, &file2}
	target := deploymentmanager.TargetConfiguration{Config: &configFile, Imports: imports}
	deployment := deploymentmanager.Deployment{Name: "software-ontap", Target: &target}

	res, err := deploymentmanagerService.Deployments.Insert(projectId, &deployment).Do()
	if err != nil {
		return err
	}
	log.Println(res)
	return nil
}

func (a *OrderActivities) OrderBanana(choice string) error {
	fmt.Printf("Order choice-multi: %v\n", choice)
	return nil
}

func (a *OrderActivities) OrderCherry(choice string) error {
	fmt.Printf("Order choice-multi: %v\n", choice)
	return nil
}

func (a *OrderActivities) OrderOrange(choice string) error {
	fmt.Printf("Order choice-multi: %v\n", choice)
	return nil
}

func (a *OrderActivities) GetBasketOrder(ctx context.Context) ([]string, error) {
	var basket []string
	for _, item := range a.OrderChoices {
		// some random decision
		if rand.Float32() <= 0.65 {
			basket = append(basket, item)
		}
	}

	if len(basket) == 0 {
		basket = append(basket, a.OrderChoices[rand.Intn(len(a.OrderChoices))])
	}

	activity.GetLogger(ctx).Info("Get basket order.", "Orders", basket)
	return basket, nil
}
