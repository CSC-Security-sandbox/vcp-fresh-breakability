package workflows

import (
	"fmt"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/active_directory_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// EnsureKerberosConfigWorkflow orchestrates the Kerberos configuration for NFSv4 volumes through individual activities
func EnsureKerberosConfigWorkflow(ctx workflow.Context, node *models.Node, ad *vsa.ActiveDirectory, svmName, externalSVMUUID string) error {
	log := util.GetLogger(ctx)
	log.Debug("Starting Kerberos configuration workflow", "svm", externalSVMUUID)

	activeDirectoryActivity := &active_directory_activities.ActiveDirectoryActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return WrapErrorForChildWorkflow(err)
	}

	// Set activity options
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Step 1: Create name mapping for krb-unix
	log.Info("Step 1: Creating name mapping for Kerberos")
	err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.CreateNameMappingForKerberosActivity, &node, externalSVMUUID, ad.Domain).Get(ctx, nil)
	if err != nil {
		log.Error("Failed to create name mapping for Kerberos", "error", err)
		return WrapErrorForChildWorkflow(err)
	}

	// Step 2: Check if Kerberos realm exists
	realm := strings.ToUpper(ad.Domain)
	log.Info("Step 2: Checking if Kerberos realm exists", "realm", realm)
	var realmExists bool
	err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.CheckKerberosRealmExistsActivity, &node, externalSVMUUID, realm).Get(ctx, &realmExists)
	if err != nil {
		log.Error("Failed to check Kerberos realm", "error", err)
		return WrapErrorForChildWorkflow(err)
	}

	// Step 3: Create Kerberos realm if it doesn't exist
	if !realmExists {
		log.Info("Step 3: Creating Kerberos realm", "realm", realm)
		kdcIP := ad.KdcIP
		if kdcIP == "" {
			err = fmt.Errorf("KDC IP is required for Kerberos realm creation but not found in Active Directory")
			log.Error("KDC IP missing", "error", err)
			return WrapErrorForChildWorkflow(err)
		}

		realmKdcPort := vsa.RealmKdcPort
		realmClockSkew := vsa.RealmClockSkew
		realmKdcVendor := vsa.RealmKdcVendor
		realmAdminServerPort := vsa.RealmAdminServerPort
		realmPasswordServerPort := vsa.RealmPasswordServerPort
		realmParams := vsa.KerberosRealmCreateParams{
			Realm:              realm,
			KdcIP:              kdcIP,
			RealmKDCPort:       &realmKdcPort,
			RealmClockSkew:     &realmClockSkew,
			RealmKDCVendor:     &realmKdcVendor,
			AdminServerIP:      &kdcIP,
			AdminServerPort:    &realmAdminServerPort,
			PasswordServerIP:   &kdcIP,
			PasswordServerPort: &realmPasswordServerPort,
			ADServerIP:         &kdcIP,
			ADServerName:       &ad.AdName,
			AdName:             ad.AdName,
			SvmUUID:            externalSVMUUID,
		}

		err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.CreateKerberosRealmActivity, &node, externalSVMUUID, realmParams).Get(ctx, nil)
		if err != nil {
			log.Error("Failed to create Kerberos realm", "error", err)
			return WrapErrorForChildWorkflow(err)
		}
		log.Info("Successfully created Kerberos realm", "realm", realm)
	} else {
		log.Info("Kerberos realm already exists", "realm", realm)
	}

	// Step 4: Create or modify AD DNS
	log.Info("Step 4: Creating or modifying AD DNS")
	err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.CreateOrModifyADDNS, &node, &ad, svmName, externalSVMUUID).Get(ctx, nil)
	if err != nil {
		log.Error("Failed to create or modify AD DNS", "error", err)
		return WrapErrorForChildWorkflow(err)
	}

	// Step 5: Get or create CIFS service to get FQDN
	log.Info("Step 5: Getting or creating CIFS service")
	var cifsServiceResult *active_directory_activities.GetOrCreateCifsServiceResult
	err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.GetOrCreateCifsService, &node, &ad, svmName, externalSVMUUID).Get(ctx, &cifsServiceResult)
	if err != nil {
		log.Error("Failed to get or create CIFS service", "error", err)
		return WrapErrorForChildWorkflow(err)
	}

	var fqdn string
	if cifsServiceResult.FQDN != "" {
		fqdn = cifsServiceResult.FQDN
	} else if cifsServiceResult.CifsServiceName != "" && cifsServiceResult.AdDomain != "" {
		fqdn = cifsServiceResult.CifsServiceName + "." + cifsServiceResult.AdDomain
	} else {
		err = fmt.Errorf("unable to determine FQDN for Kerberos configuration")
		log.Error("Unable to determine FQDN", "error", err)
		return WrapErrorForChildWorkflow(err)
	}
	log.Info("FQDN determined", "fqdn", fqdn)

	// Step 6: Get data LIFs for the SVM
	log.Info("Step 6: Getting data LIFs for SVM")
	var dataLifs []*ontapRest.IPInterface
	err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.GetDataLifsForSVMActivity, &node, externalSVMUUID, svmName).Get(ctx, &dataLifs)
	if err != nil {
		log.Error("Failed to get data LIFs for SVM", "error", err, "svm", externalSVMUUID)
		return WrapErrorForChildWorkflow(err)
	}

	if len(dataLifs) == 0 {
		log.Warn("No data LIFs found for SVM, skipping Kerberos interface configuration", "svm", externalSVMUUID)
		return nil
	}
	log.Info("Found data LIFs", "count", len(dataLifs))

	// Step 7: Enable Kerberos on each NAS LIF
	nasLifName := svmName + "-ilbnas"
	log.Info("Step 7: Enabling Kerberos on NAS LIFs", "expectedLifPattern", nasLifName)
	for _, dataLif := range dataLifs {
		if dataLif.Name != nil && strings.Contains(strings.ToLower(*dataLif.Name), strings.ToLower(nasLifName)) {
			actualLifName := *dataLif.Name
			log.Info("Configuring Kerberos for NAS LIF", "nasLifName", actualLifName)
			if dataLif.IP == nil || dataLif.IP.Address == nil {
				err = fmt.Errorf("IP address not found for NAS LIF: %s", actualLifName)
				log.Error("IP address missing", "error", err, "nasLifName", actualLifName)
				return WrapErrorForChildWorkflow(err)
			}
			nasLifIP := string(*dataLif.IP.Address)
			err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.EnableKerberosOnInterfaceActivity, &node, externalSVMUUID, svmName, actualLifName, nasLifIP, fqdn, realm, &ad).Get(ctx, nil)
			if err != nil {
				log.Error("Failed to enable Kerberos on interface", "error", err, "dataLif", actualLifName)
				return WrapErrorForChildWorkflow(err)
			}
			log.Info("Successfully enabled Kerberos on interface", "dataLif", actualLifName)
		}
	}

	log.Info("Successfully configured Kerberos for NFSv4", "svm", externalSVMUUID, "fqdn", fqdn)
	return nil
}
